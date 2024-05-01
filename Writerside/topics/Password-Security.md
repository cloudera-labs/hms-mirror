# Password Security

## Secure Passwords in Configuration

There are two passwords stored in the configuration file mentioned above.  One for each 'JDBC connection, if those rely on a password for connect.  By default, the passwords are in clear text in the configuration file.  This usually isn't an issue since the file can be protected at the UNIX level from peering eyes.  But if you need to protect those passwords, `hms-mirror` supports storing an encrypted version of the password in the configuration.

The `password` element for each JDBC connection can be replaced with an **encrypted** version of the password and read by `hms-mirror` during execution, so the clear text version of the password isn't persisted anywhere.

When you're using this feature, you need to have a `password-key`.  This is a key used to encrypt and decrypt the 
password in the configuration.  The **same** `password-key` must be used for **ALL** passwords in the configuration 
file.

## Generate the Encrypted Password

Use the `-pkey` and `-p` options of `hms-mirror`

`hms-mirror -pkey cloudera -p have-a-nice-day`

Will generate:
```
=== Errors ===
	38:Password en/de crypt

=== Warnings ===
	56:Encrypted password: HD1eNF8NMFahA2smLM9c4g==
```

Ignore the error 38, it's just a warning that the password is being encrypted.  The encrypted password is the value after the `Encrypted password:` string.

Copy this encrypted password and place it in your configuration file for the JDBC connection.  Repeat for the other passwords, if it's different, and paste it in the configuration as well.

## Running `hms-mirror` with Encrypted Passwords

Using the **same** `-pkey` you used to generate the encrypted password, we'll run `hms-mirror`

`hms-mirror -db <db> -pkey cloudera ...`

When the `-pkey` option is specified **WITHOUT** the `-p` option (used previously), `hms-mirror` will understand to **decrypt** the configuration passwords before connecting to jdbc.  If you receive jdbc connection exceptions, recheck the `-pkey` and encrypted password from before.

## Testing the Encrypted Password

If you're unsure if the password is being decrypted correctly, you can use the `-dp` option to decrypt the hashed 
password and print it to the console.

`hms-mirror -pkey cloudera -dp HD1eNF8NMFahA2smLM9c4g==`

Will generate:
```
=== Errors ===
	38:Password en/de crypt

=== Warnings ===
	57:Decrypted password: have-a-nice-day
```

Again, ignore the error 38, it's just a warning that the password is being decrypted.  The decrypted password is the value after the `Decrypted password:` string.

This should match the password you used to generate the encrypted password.