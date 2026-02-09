"""TLS certificate handling for CDP environment.

CDP provides custom CA certificates as base64-encoded environment variables
with the prefix `TRUSTSTORE_`. This module extracts and loads them for use
with database connections and other TLS-enabled services.
"""

import base64
import binascii
import logging
import os
import tempfile

logger = logging.getLogger(__name__)

# Global certificate store - maps TRUSTSTORE_* names to file paths
custom_ca_certs: dict[str, str] = {}


def extract_all_certs() -> dict[str, str]:
    """Extract all TRUSTSTORE_* certificates from environment variables.

    CDP passes custom CA certificates as base64-encoded environment variables.
    This function decodes them and writes to temporary files.

    Returns:
        Dict mapping TRUSTSTORE_* names to temporary file paths containing the certs.
    """
    certs = {}
    for var_name, var_value in os.environ.items():
        if var_name.startswith("TRUSTSTORE_"):
            try:
                decoded_value = base64.b64decode(var_value)
            except binascii.Error as err:
                logger.error("Error decoding certificate %s: %s", var_name, err)
                continue

            with tempfile.NamedTemporaryFile(
                mode="wb", delete=False, prefix=var_name, suffix=".pem"
            ) as tmp_file:
                tmp_file.write(decoded_value)
                certs[var_name] = tmp_file.name
                logger.info("Extracted certificate %s to %s", var_name, tmp_file.name)

    logger.info("Loaded %d custom certificates from environment", len(certs))
    return certs


def init_custom_certificates() -> dict[str, str]:
    """Initialize custom certificates from CDP environment.

    Call this at application startup to extract certificates.
    The certificates are stored in the global `custom_ca_certs` dict.

    Returns:
        Dict mapping TRUSTSTORE_* names to temporary file paths.
    """
    global custom_ca_certs
    logger.info("Initializing custom TLS certificates")
    custom_ca_certs = extract_all_certs()
    return custom_ca_certs


def get_cert_path(truststore_name: str) -> str | None:
    """Get the file path for a specific truststore certificate.

    Args:
        truststore_name: The TRUSTSTORE_* environment variable name,
                        e.g., "TRUSTSTORE_RDS" or just "RDS".

    Returns:
        Path to the certificate file, or None if not found.
    """
    # Allow passing with or without prefix
    if not truststore_name.startswith("TRUSTSTORE_"):
        truststore_name = f"TRUSTSTORE_{truststore_name}"

    cert_path = custom_ca_certs.get(truststore_name)
    if cert_path:
        logger.debug("Found certificate for %s: %s", truststore_name, cert_path)
    else:
        logger.debug("No certificate found for %s", truststore_name)
    return cert_path


# Initialize certificates on module import
init_custom_certificates()
