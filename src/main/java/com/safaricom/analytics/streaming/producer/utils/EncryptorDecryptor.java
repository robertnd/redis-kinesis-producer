package com.safaricom.analytics.streaming.producer.utils;


import com.safaricom.microservices.libs.encryption.AESUtility;


/**
 * Utility security class for encryption and decryption.
 */
public class EncryptorDecryptor extends AESUtility {
    private EncryptorDecryptor() {
        // Create a constructor
    }

    /**
     * Decryptor string.
     *
     * @param appProfile    the app profile
     * @param appVersion    the app version
     * @param ivKey         the iv key
     * @param encryptedText the encrypted text
     * @return the string
     */
    public static String decryptor(String appProfile, String appVersion,
                                   String ivKey, String encryptedText) {
        EncryptorDecryptor encryptorDecryptor = new EncryptorDecryptor();
        return encryptorDecryptor.decryptPayload(appProfile, appVersion,
                ivKey, encryptedText);
    }

    /**
     * Encryptor string.
     *
     * @param appProfile    the app profile
     * @param appVersion    the app version
     * @param ivKey         the iv key
     * @param encryptedText the encrypted text
     * @return the string
     */
    public static String encryptor(String appProfile, String appVersion,
                                   String ivKey, String encryptedText) {
        EncryptorDecryptor encryptorDecryptor = new EncryptorDecryptor();
        return encryptorDecryptor.encryptPayload(appProfile, appVersion, ivKey, encryptedText);
    }
}


