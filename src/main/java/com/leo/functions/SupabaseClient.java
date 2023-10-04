package com.leo.functions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hc.client5.http.classic.methods.HttpPatch;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;

import java.util.HashMap;
import java.util.Map;

public class SupabaseClient {
    private final String email;
    private final String password;
    private final String apiKey;
    private final String uri;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private String accessToken;

    public SupabaseClient(String email, String password, String apiKey, String uri) {
        this.email = email;
        this.password = password;
        this.apiKey = apiKey;
        this.uri = uri;
    }

    public void login() throws SupabaseException {
        // Login
        final Map<String, Object> body = new HashMap<>();
        body.put("email", email);
        body.put("password", password);
        body.put("gotrue_meta_security", new HashMap<>());
        final HttpPost post = new HttpPost(this.uri + "/auth/v1/token?grant_type=password");
        post.addHeader("Apikey", this.apiKey);
        try {
            post.setEntity(new StringEntity(new ObjectMapper().writeValueAsString(body)));
            try (CloseableHttpClient httpClient = HttpClients.createDefault(); CloseableHttpResponse response = httpClient.execute(post)) {
                final String rawResult = EntityUtils.toString(response.getEntity());
                if(response.getCode() == 200) {
                    final JsonNode result = objectMapper.readTree(rawResult);
                    this.accessToken = result.get("access_token").asText();
                } else {
                    throw new SupabaseException("cannot.log.in : " + rawResult);
                }
            } catch (Exception e) {
                throw new SupabaseException("cannot.log.in", e);
            }
        } catch (JsonProcessingException e) {
            throw new SupabaseException("cannot.serialize", e);
        }
    }

    public void updateJobStateAndProgress(final String jobUri, final String state, final int progress) throws SupabaseException {
        final Map<String, Object> body = new HashMap<>();
        body.put("state", state);
        body.put("progress", progress);
        final HttpPatch request = new HttpPatch(this.uri + "/rest/v1/jobs?uri=eq." + jobUri);
        request.addHeader("Content-Type", "application/json");
        request.addHeader("Apikey", this.apiKey);
        request.addHeader("Authorization", "Bearer " + this.accessToken);
        try {
            request.setEntity(new StringEntity(new ObjectMapper().writeValueAsString(body)));
            try (CloseableHttpClient httpClient = HttpClients.createDefault(); CloseableHttpResponse response = httpClient.execute(request)) {
                final int code = response.getCode();
                if(code >= 200 && code <= 299) {
                    System.out.println("Job " + jobUri + " successfully updated");
                } else {
                    System.err.println("Job " + jobUri + " not updated : " + EntityUtils.toString(response.getEntity()));
                }
            } catch (Exception e) {
                throw new SupabaseException("cannot.update.job", e);
            }
        } catch (JsonProcessingException e) {
            throw new SupabaseException("cannot.serialize", e);
        }
    }

}
