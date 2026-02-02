package uz.greenwhite.gateway.util;

import org.springframework.http.HttpHeaders;

public class AuthUtil {

    public static String generateBasicAuth(String username, String password) {
        String code = HttpHeaders.encodeBasicAuth(username, password, null);
        return "Basic " + code;
    }
}