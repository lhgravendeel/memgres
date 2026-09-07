package com.memgres.engine;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;

import javax.crypto.Mac;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * The verifier PostgreSQL stores for a role's password.
 *
 * <p>A password is never kept as it was written: PostgreSQL runs it through SCRAM-SHA-256 and
 * stores the result, which is what {@code pg_authid.rolpassword} holds and what a reader checks
 * the method of. The text is
 * {@code SCRAM-SHA-256$<iterations>:<salt>$<stored key>:<server key>}, each key base64.
 *
 * <p>The verifier is worked out once, where the password is given, and kept. Worked out again at
 * every catalogue read it would carry a different salt each time, so two reads of one role would
 * disagree; and the password itself would have to be kept to do it, which is the thing this
 * avoids.
 */
final class PgScramVerifier {

    private PgScramVerifier() {
    }

    /** What PostgreSQL uses, and what it writes into the verifier it stores. */
    private static final int ITERATIONS = 4096;

    /** The salt is sixteen bytes, as PostgreSQL's own is. */
    private static final int SALT_BYTES = 16;

    private static final SecureRandom RANDOM = new SecureRandom();

    /**
     * Whether the text is already a verifier rather than a password. PostgreSQL stores one written
     * this way as it stands, so a dump that carries verifiers reloads without turning each of them
     * into the password of a second verifier.
     */
    static boolean isVerifier(String written) {
        return written != null
                && (written.startsWith("SCRAM-SHA-256$") || written.startsWith("md5"));
    }

    /** The verifier for a password, or the text itself when it is already one. */
    static String of(String password) {
        if (password == null) return null;
        if (isVerifier(password)) return password;
        byte[] salt = new byte[SALT_BYTES];
        RANDOM.nextBytes(salt);
        try {
            byte[] salted = saltedPassword(password, salt);
            byte[] clientKey = hmac(salted, "Client Key");
            byte[] storedKey = MessageDigest.getInstance("SHA-256").digest(clientKey);
            byte[] serverKey = hmac(salted, "Server Key");
            Base64.Encoder b64 = Base64.getEncoder();
            return "SCRAM-SHA-256$" + ITERATIONS + ":" + b64.encodeToString(salt)
                    + "$" + b64.encodeToString(storedKey) + ":" + b64.encodeToString(serverKey);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            // Every algorithm named here is one the platform is required to carry, so this cannot
            // happen; a password that could not be encrypted is not one to store in the clear.
            throw new MemgresException("password encryption failed", "XX000");
        }
    }

    private static byte[] saltedPassword(String password, byte[] salt) throws Exception {
        PBEKeySpec spec = new PBEKeySpec(
                password.toCharArray(), salt, ITERATIONS, 256);
        return SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256").generateSecret(spec)
                .getEncoded();
    }

    private static byte[] hmac(byte[] key, String message) throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(key, "HmacSHA256"));
        return mac.doFinal(message.getBytes(StandardCharsets.UTF_8));
    }
}
