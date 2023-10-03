package com.leo.functions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.functions.HttpFunction;
import com.google.cloud.functions.HttpRequest;
import com.google.cloud.functions.HttpResponse;
import com.google.cloud.storage.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class ClipperFunction implements HttpFunction {

    private static final ObjectMapper mapper = new ObjectMapper();

    private final Storage storage;

    private final String workingDir;

    private final String bucketName;
    private final long timeout;

    private final int maxDuration;

    private static final List<String> domains = Arrays.stream(System.getenv("CORS_DOMAINS")
                    .split(","))
            .filter(s -> s != null && !s.trim().isEmpty())
            .map(String::trim)
            .distinct().collect(Collectors.toList());

    public ClipperFunction() {
        workingDir = System.getenv("WORKING_DIR");
        final String projectId = System.getenv("PROJECT_ID");
        bucketName = System.getenv("BUCKET_NAME");
        storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
        final String strTimeout = System.getenv("CLIP_TIMEOUT");
        if(strTimeout == null || strTimeout.isBlank()) {
            timeout = 120000;
        } else {
            timeout = Long.parseLong(strTimeout);
        }
        final String strMaxDuration = System.getenv("MAX_DURATION");
        if(strMaxDuration == null || strMaxDuration.isBlank()) {
            maxDuration = 10;
        } else {
            maxDuration = Integer.parseInt(strMaxDuration);
        }
    }

    @Override
    public void service(HttpRequest request, HttpResponse response)
            throws IOException {
        final String requestOrigin = request.getFirstHeader("origin").orElse("");
        response.appendHeader("Access-Control-Allow-Origin", getAllowOrigin(requestOrigin));
        response.appendHeader("Access-Control-Allow-Methods", "GET, POST");
        response.appendHeader("Access-Control-Allow-Headers", "*");

        if ("OPTIONS".equalsIgnoreCase(request.getMethod())) {
            // stop preflight requests here
            response.setStatusCode(204);
            response.getOutputStream().close();
            return;
        }
        response.setContentType("application/json");
        final ClipperRequest clipperRequest = mapper.readValue(request.getInputStream(), ClipperRequest.class);
        final ClipperResponse clipperResponse = process(clipperRequest);
        mapper.writeValue(response.getWriter(), clipperResponse);
    }

    private ClipperResponse process(ClipperRequest clipperRequest) {
        final List<PartResponse> partResponses = clipperRequest.parts.stream().collect(
                Collectors.groupingBy(p -> p.video))
        .entrySet().stream().flatMap(videosParts -> {
            final String videoName = videosParts.getKey();
            getVideo(videoName);
            return cutVideo(videoName, videosParts.getValue()).stream();
        }).collect(Collectors.toList());
        return new ClipperResponse(partResponses, partResponses.stream().anyMatch(resp -> !resp.succeeded));
    }

    private List<PartResponse> cutVideo(String videoName, List<PartRequest> parts) {
        final String sourcePath = Paths.get(workingDir, videoName + ".mp4").toAbsolutePath().toString();
        return parts.stream()
            .map(part -> {
                final int from = part.from;
                final int to = part.to;
                final String blobName = getPartObjectName(videoName, from, to);
                boolean succeeded = false;
                if(to <= from) {
                    System.err.println("Error while clipping " + videoName + " from " + from + " to " + to + ": boundaries reversed");
                } else if(to - from > maxDuration) {
                    System.err.println("Error while clipping " + videoName + " from " + from + " to " + to + ": range too large");
                } else if(storage.get(bucketName, blobName) == null) {
                    final Path pathToPartFile = Paths.get(workingDir, videoName + "_" + from + "_" + to + ".mp4");
                    final Process process;
                    try {
                        process = Runtime.getRuntime()
                                .exec("ffmpeg -i " + sourcePath + " -ss " + from + " -to " + to + " -c:v libx264 " + pathToPartFile.toAbsolutePath().toString());
                        final BufferedReader reader = new BufferedReader(
                                new InputStreamReader(process.getInputStream()));
                        String line;
                        while ((line = reader.readLine()) != null) {
                            System.out.println("[FFMPEG] " + line);
                        }
                        final boolean exited = process.waitFor(timeout, TimeUnit.MILLISECONDS);
                        if (exited && process.exitValue() == 0) {
                            succeeded = uploadPart(videoName, from, to, pathToPartFile);
                        } else {
                            System.err.println("Error while clipping " + videoName + " from " + from + " to " + to + " ffmpeg");
                        }
                    } catch (IOException | InterruptedException e) {
                        System.err.println("Error while clipping " + videoName + " from " + from + " to " + to);
                        e.printStackTrace();
                    }
                } else {
                    System.out.println("Part already clipped : " + videoName + " from " + from + " to " + to);
                }
                return new PartResponse(videoName, from, to, succeeded);
            }).collect(Collectors.toList());
    }

    private String getPartObjectName(String videoName, int from, int to) {
        return "videos/" + videoName + "/" + videoName + "_" + from + "_" + to + ".mp4";
    }

    private boolean uploadPart(String videoName, int from, int to, Path pathToPartFile) {
        System.out.println("Uploading part of : " + videoName + " from " + from + " to " + to);
        final String blobName = getPartObjectName(videoName, from, to);
        final BlobId blobId = BlobId.of(bucketName, blobName);
        final BlobInfo blobInfo = BlobInfo.newBuilder(blobId)
                .setContentType("video/mp4")
                .setCacheControl("public, max-age=31536000")
                .build();
        final Storage.BlobWriteOption precondition = Storage.BlobWriteOption.doesNotExist();
        try {
            storage.createFrom(blobInfo, pathToPartFile, precondition);
            System.out.println("upload done");
            Files.deleteIfExists(pathToPartFile);
            return true;
        } catch (IOException e) {
            System.err.println("Error while clipping " + videoName + " from " + from + " to " + to);
            e.printStackTrace();
            return false;
        }
    }



    private void getVideo(String videoName) {
        Path targetPath = Paths.get(workingDir, videoName + ".mp4");
        if(Files.exists(targetPath)) {
            System.out.println(targetPath + " already downloaded");
        } else {
            System.out.println("Download " + videoName + "...");
            final Blob blob = storage.get(BlobId.of(bucketName, "videos/" + videoName + "/" + videoName + ".mp4"));
            blob.downloadTo(targetPath);
            System.out.println("..." + videoName + " downloaded");
        }
    }

    private static String getAllowOrigin(final String requestOrigin) {
        if(requestOrigin == null || requestOrigin.isBlank()) {
            return domains.get(0);
        }
        if(requestOrigin.indexOf("http://localhost") == 0 || domains.contains(requestOrigin)) {
            return requestOrigin;
        }
        return domains.get(0);
    }


    public static class ClipperRequest {
        public List<PartRequest> parts;

    }

    public static class PartRequest {
        public String video;
        public int from;
        public int to;
    }

    public static class ClipperResponse{
        public List<PartResponse> parts;
        public boolean hasErrors;

        public ClipperResponse(List<PartResponse> parts, boolean hasErrors) {
            this.parts = parts;
            this.hasErrors = hasErrors;
        }
    }

    public static class PartResponse {
        public String video;
        public int from;
        public int to;
        public boolean succeeded;

        public PartResponse(String video, int from, int to, boolean succeeded) {
            this.video = video;
            this.from = from;
            this.to = to;
            this.succeeded = succeeded;
        }
    }
    public static class StreamGobbler implements Runnable {
        private InputStream inputStream;
        private Consumer<String> consumer;

        public StreamGobbler(InputStream inputStream, Consumer<String> consumer) {
            this.inputStream = inputStream;
            this.consumer = consumer;
        }

        @Override
        public void run() {
            new BufferedReader(new InputStreamReader(inputStream)).lines()
                    .forEach(consumer);
        }
    }
}
