package com.leo.functions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.functions.HttpFunction;
import com.google.cloud.functions.HttpRequest;
import com.google.cloud.functions.HttpResponse;
import com.google.cloud.storage.*;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteWatchdog;

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
    private final SupabaseClient supabaseClient;

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
        this.supabaseClient = new SupabaseClient(
                getMandatoryEnv("SUPABASE_USER_EMAIL"),
                getMandatoryEnv("SUPABASE_USER_PWD"),
                getMandatoryEnv("SUPABASE_API_KEY"),
                getMandatoryEnv("SUPABASE_URL")
        );
    }

    private String getMandatoryEnv(String key) {
        final String value = System.getenv(key);
        if(value == null || value.isBlank()) {
            throw new RuntimeException("missing.env.var." + key);
        }
        return value;
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
            return cutVideo(videoName, videosParts.getValue(), clipperRequest.accessToken).stream();
        })
        .collect(Collectors.toList());
        updateJobs(partResponses);
        return new ClipperResponse(partResponses, partResponses.stream().anyMatch(resp -> !resp.succeeded));
    }

    private void updateJobs(List<PartResponse> partResponses) {
        try {
            supabaseClient.login();
            for (PartResponse partResponse : partResponses) {
                try {
                    final String state = partResponse.succeeded ? "OK" : "KO";
                    supabaseClient.updateJobStateAndProgress(partResponse.video + "_" + partResponse.from + "_" + partResponse.to + ".mp4", state, 100);
                } catch (SupabaseException se) {

                }
            }
        } catch (SupabaseException e) {
            System.err.println("An error occurred while updating job statuses");
            e.printStackTrace();
        }
    }

    private List<PartResponse> cutVideo(final String videoName,
                                        final List<PartRequest> parts,
                                        final String accessToken) {
        final String videoUrl = "https://" + bucketName + ".storage.googleapis.com/videos/" + videoName + "/" + videoName + ".mp4?access_token=" + accessToken;
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
                    System.out.println("Clipping " +videoName + "_" + from + "_" + to);
                    final Path pathToPartFile = Paths.get(workingDir, videoName + "_" + from + "_" + to + ".mp4");
                    final CommandLine commandLine = new CommandLine("ffmpeg")
                            .addArgument("-ss")
                            .addArgument(String.valueOf(from))
                            .addArgument("-to")
                            .addArgument(String.valueOf(to))
                            .addArgument("-i")
                            .addArgument(videoUrl)
                            .addArgument("-c:v")
                            .addArgument("libx264")
                            .addArgument(pathToPartFile.toAbsolutePath().toString());
                    try {
                        DefaultExecutor executor = new DefaultExecutor();
                        executor.setExitValue(0);
                        ExecuteWatchdog watchdog = new ExecuteWatchdog(60000);
                        executor.setWatchdog(watchdog);
                        int exitValue = executor.execute(commandLine);
                        if (exitValue == 0) {
                            System.out.println("Clipping succeeded");
                            succeeded = uploadPart(videoName, from, to, pathToPartFile);
                        } else {
                            System.err.println("Error while clipping " + videoName + " from " + from + " to " + to + " ffmpeg");
                        }
                    } catch (Exception e) {
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
        public String accessToken;
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
