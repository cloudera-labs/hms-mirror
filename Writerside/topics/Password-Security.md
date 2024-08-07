# Password Security

## Secure Passwords in Configuration

There are multiple passwords stored in the configuration files. By default, the passwords are in clear text in the configuration file. This usually isn't an issue since the file can be protected at the UNIX level from peering eyes. But if you need to protect those passwords, `hms-mirror` supports storing an encrypted version of the password in the configuration.

When you're using this feature, you need to have a `password-key`. This is a key used to encrypt and decrypt the
password in the configuration. The **same** `password-key` must be used for **ALL** passwords in the configuration
file.


## WEB UI

Passwords are saved in the configuration and can easily be encrypted and decrypted using the Web UI. If the password
(s) are encrypted, the 'Passwords Encrypted' checkbox will be checked.

![pwd_mngd.png](pwd_mngd.png)

If the passwords are encrypted, you'll need to specify the 'Encrypt Key' before running or connection to any 
endpoints. Set the 'Encrypt Key' and click 'Save' to save the key for the session.

> The 'Encrypt Key' is only used for the current session and will NOT be saved if you 'persist' the session.


Once encrypted, you'll need to specify the 'password key' with that session to decrypt them for use.


## CLI

### Generate the Encrypted Password

Use the `-pkey` and `-p` options of `hms-mirror` to generate and decrypt the password(s).

`hms-mirror -pkey cloudera -p have-a-nice-day`

Will generate:

```
=== Errors ===
	38:Password en/de crypt

=== Warnings ===
	56:Encrypted password: HD1eNF8NMFahA2smLM9c4g==
```

Ignore the error 38, it's just a warning that the password is being encrypted. The encrypted password is the value after the `Encrypted password:` string.

Copy this encrypted password and place it in your configuration file for the JDBC connection. Repeat for the other passwords, if it's different, and paste it in the configuration as well.

### Running `hms-mirror` with Encrypted Passwords

Using the **same** `-pkey` you used to generate the encrypted password, we'll run `hms-mirror`

`hms-mirror -db <db> -pkey cloudera ...`

When the `-pkey` option is specified **WITHOUT** the `-p` option (used previously), `hms-mirror` will understand to *
*decrypt
** the configuration passwords before connecting to jdbc. If you receive jdbc connection exceptions, recheck the `-pkey` and encrypted password from before.

### Testing the Encrypted Password

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

Again, ignore the error 38, it's just a warning that the password is being decrypted. The decrypted password is the value after the `Decrypted password:` string.

This should match the password you used to generate the encrypted password.


