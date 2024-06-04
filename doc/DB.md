# Databases

## Redis

DB 0: certificats et tokens
DB 1: ?
DB 2: sessions
DB 3: maitre des cles local (obsolete?)
DB 4: 

## Redis database

Base de donnes locale à l'instance. Le contenu est volatil (cache).

Database 0 : certificats

Key : certificat_[VERSION]:[fingerprint]
Value : 
Exemple :
certificat_v1:e47052a4ec2461152f0efe577305a0dc16836dfea36b4bd7d5ec6f875b0497b6
{
  "pems": [
    "-----BEGIN CERTIFICATE-----\\nMIICkTCCAkOgAwIBAgIUZWqJz7ZyURV61reFFzUoc8awecMwBQYDK2VwMHIxLTAr\\nBgNVBAMTJDVjYjlkNTBlLTExMGUtNDIxYS1iOTQ0LTUyM2I2MGFmNGMzYjFBMD8G\\nA1UEChM4emVZbmNScUVxWjZlVEVtVVo4d2hKRnVIRzc5NmVTdkNUV0U0TTQzMml6\\nWHJwMjJiQXR3R203SmYwHhcNMjQwNTIzMTY0ODA4WhcNMjQwNjIzMTY0ODI4WjCB\\nijEtMCsGA1UEAwwkNWNiOWQ1MGUtMTEwZS00MjFhLWI5NDQtNTIzYjYwYWY0YzNi\\nMRYwFAYDVQQLDA1tYWl0cmVjb21wdGVzMUEwPwYDVQQKDDh6ZVluY1JxRXFaNmVU\\nRW1VWjh3aEpGdUhHNzk2ZVN2Q1RXRTRNNDMyaXpYcnAyMmJBdHdHbTdKZjAqMAUG\\nAytlcAMhAORwUqTsJGEVLw7+V3MFoNwWg23+o2tL19Xsb4dbBJe2o4HRMIHOMBgG\\nBCoDBAAEEDIucHJpdmUsMS5wdWJsaWMwFQYEKgMEAQQNbWFpdHJlY29tcHRlczBb\\nBgNVHREEVDBSgh50aGlua2NlbnRyZTEubWFwbGUubWFjZXJvYy5jb22CDW1haXRy\\nZWNvbXB0ZXOCCWxvY2FsaG9zdIcEfwAAAYcQAAAAAAAAAAAAAAAAAAAAATAfBgNV\\nHSMEGDAWgBTh3aSCmRxErlRkdIbxAtzT+2cQSDAdBgNVHQ4EFgQUwhKUc/anHPLe\\n3+j/o08tjtij1ykwBQYDK2VwA0EANDcZLDxXWzySBLNQZctJGyPgNIpfiTLs0AGF\\ncTO2tyP2cu+wqW/GvYiWkF0gaZFEGUa4nkoQ9Tff6A0/CrUdBQ==\\n-----END CERTIFICATE-----\\n",
    "-----BEGIN CERTIFICATE-----\\nMIIBozCCAVWgAwIBAgIKARQoU5IBSUJYIDAFBgMrZXAwFjEUMBIGA1UEAxMLTWls\\nbGVHcmlsbGUwHhcNMjQwNTIzMTY0NzI0WhcNMjUxMjAyMTY0NzI0WjByMS0wKwYD\\nVQQDEyQ1Y2I5ZDUwZS0xMTBlLTQyMWEtYjk0NC01MjNiNjBhZjRjM2IxQTA/BgNV\\nBAoTOHplWW5jUnFFcVo2ZVRFbVVaOHdoSkZ1SEc3OTZlU3ZDVFdFNE00MzJpelhy\\ncDIyYkF0d0dtN0pmMCowBQYDK2VwAyEAMk45FqcLdEcsny15TLoUHS7IG0TLeXox\\nsCPCGxO+DFajYzBhMBIGA1UdEwEB/wQIMAYBAf8CAQAwCwYDVR0PBAQDAgEGMB0G\\nA1UdDgQWBBTh3aSCmRxErlRkdIbxAtzT+2cQSDAfBgNVHSMEGDAWgBTTiP/MFw4D\\nDwXqQ/J2LLYPRUkkETAFBgMrZXADQQDz2n0VmNvRLlcXt6QGnJEb3WqCGwfIdjvN\\nUqaXcX6OIpEJ3OquAQpZmuOSXIxVRd6mxznvceNoARJOOZi2NQwK\\n-----END CERTIFICATE-----\\n"
  ],
  "ca": null
}

Database 2 : sessions

Session millegrille
Key : mgsession.Mjn48Bph7VMl0w_IbZqp79o_BQAJceFnZajh42lfV8g
Value :
{
  "user_id": "z2i3XjxM2VA9wGr8s3vVdfNpv3tL7XwADo6YbWe8dgXuQUStLZa",
  "hostname": "thinkcentre1.maple.maceroc.com",
  "challenge": "Mjn48Bph7VMl0w_IbZqp79o_BQAJceFnZajh42lfV8g",
  "expiration": 1717584330,
  "nomUsager": "proprietaire"
}

Session aiohttp
Key : auth.aiohttp_7921068508b74c788097805da46c95d7
Value :
{
  "created": 1717497930,
  "session": {
    "userIdChallenge": "z2i3XjxM2VA9wGr8s3vVdfNpv3tL7XwADo6YbWe8dgXuQUStLZa",
    "userNameChallenge": "proprietaire",
    "authentifiee": true,
    "userId": "z2i3XjxM2VA9wGr8s3vVdfNpv3tL7XwADo6YbWe8dgXuQUStLZa",
    "userName": "proprietaire",
    "passkey_authentication": {
      "ast": {
        "credentials": [
          {
            "cred_id": "UFKkeZWrtt_Vaj_h8B4ND5IklBTc7PJTNw_hj64gWMpHOjQ64ZI0Aq44DpUUBarDNoLgPREeh7CVCRbGrfCjRmQ8KWFD1YdUHBl78kFNbH7jkYfQ1OOLR1QNuhfi8BX1",
            "cred": {
              "type_": "ES256",
              "key": {
                "EC_EC2": {
                  "curve": "SECP256R1",
                  "x": "86PEXW8qIGufVD-W3oV8mhrIPftZY0IQ8RY992A-o9I",
                  "y": "IqBqKADv8QT6l2uqau_O-s8I5fdfzS2OfuCiflpptwM"
                }
              }
            },
            "counter": 0,
            "transports": null,
            "user_verified": false,
            "backup_eligible": false,
            "backup_state": false,
            "registration_policy": "preferred",
            "extensions": {
              "cred_protect": "NotRequested",
              "hmac_create_secret": "NotRequested",
              "appid": "NotRequested",
              "cred_props": "Ignored"
            },
            "attestation": {
              "data": "None",
              "metadata": "None"
            },
            "attestation_format": "None"
          }
        ],
        "policy": "preferred",
        "challenge": "9SyRpnLFkD6f--YyCO2EQPBV1jdiNNj2ZAqsI6nUwU8",
        "appid": null,
        "allow_backup_eligible_upgrade": true
      }
    }
  }
}

Database 3
Tokens locaux

Hebergement
Key : hebergement:[instance_id local]
Value : {"jwt_readonly":[JWTRO], "jwt_readwrite":[JWTRW], "url": "[URL consignation]"}
Expiration : date d'expiration du token - 30 secondes

