#!/usr/bin/env bats
# Cloud KMS (gcloud kms) integration tests

setup_file() {
    load 'test_helper/common-setup'
    export KEYRING="$(unique_name gcloud-kr)"
    export CRYPTOKEY="$(unique_name gcloud-key)"
    gcloud kms keyrings create "$KEYRING" --location="${FLOCI_GCP_LOCATION}" >/dev/null 2>&1
    gcloud kms keys create "$CRYPTOKEY" --location="${FLOCI_GCP_LOCATION}" \
        --keyring="$KEYRING" --purpose=encryption >/dev/null 2>&1
}

setup() {
    load 'test_helper/common-setup'
}

@test "kms: key ring appears in list" {
    run gcloud_cmd kms keyrings list --location="${FLOCI_GCP_LOCATION}" --format="value(name)"
    assert_success
    assert_output --partial "keyRings/${KEYRING}"
}

@test "kms: crypto key appears in list" {
    run gcloud_cmd kms keys list --location="${FLOCI_GCP_LOCATION}" --keyring="$KEYRING" \
        --format="value(name)"
    assert_success
    assert_output --partial "cryptoKeys/${CRYPTOKEY}"
}

@test "kms: describe crypto key reports ENCRYPT_DECRYPT purpose" {
    run gcloud_cmd kms keys describe "$CRYPTOKEY" --location="${FLOCI_GCP_LOCATION}" \
        --keyring="$KEYRING" --format="value(purpose)"
    assert_success
    assert_output --partial "ENCRYPT_DECRYPT"
}

# gcloud sends a CRC32C of every input and rejects the response unless the
# matching verified*Crc32c flag comes back, so this round trip exercises the
# REST checksum path end to end.
@test "kms: encrypt then decrypt round-trips the plaintext with AAD" {
    printf 'floci gcloud round trip' > "$BATS_TEST_TMPDIR/plain.txt"
    printf 'context' > "$BATS_TEST_TMPDIR/aad.txt"

    run gcloud_cmd kms encrypt --location="${FLOCI_GCP_LOCATION}" --keyring="$KEYRING" \
        --key="$CRYPTOKEY" --plaintext-file="$BATS_TEST_TMPDIR/plain.txt" \
        --additional-authenticated-data-file="$BATS_TEST_TMPDIR/aad.txt" \
        --ciphertext-file="$BATS_TEST_TMPDIR/cipher.bin"
    assert_success

    run gcloud_cmd kms decrypt --location="${FLOCI_GCP_LOCATION}" --keyring="$KEYRING" \
        --key="$CRYPTOKEY" --ciphertext-file="$BATS_TEST_TMPDIR/cipher.bin" \
        --additional-authenticated-data-file="$BATS_TEST_TMPDIR/aad.txt" \
        --plaintext-file="$BATS_TEST_TMPDIR/decrypted.txt"
    assert_success

    run cat "$BATS_TEST_TMPDIR/decrypted.txt"
    assert_output "floci gcloud round trip"
}
