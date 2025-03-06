using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net.Http;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;

namespace InfluxDB3.Client.Internal;

using ValidationCallback = Func<HttpRequestMessage, X509Certificate2?, X509Chain?, SslPolicyErrors, bool>;

internal static class ServerCertificateCustomValidations
{
    /// <summary>
    /// Create a ServerCertificateCustomValidationCallback function that ignores certificate validation errors.
    /// </summary>
    internal static ValidationCallback CreateSkipValidationCallback()
    {
        return (_, _, _, _) => true;
    }

    /// <summary>
    /// Create a ServerCertificateCustomValidationCallback function that uses additional root certificates for validation.
    /// </summary>
    internal static ValidationCallback CreateCustomCertificatesValidationCallback(string customCertsFilePath)
    {
        // Check custom certificates file
        if (!File.Exists(customCertsFilePath))
        {
            throw new ArgumentException($"Certificate file '{customCertsFilePath}' not found.");
        }

        var fileInfo = new FileInfo(customCertsFilePath);
        if (fileInfo.Length == 0)
        {
            throw new ArgumentException($"Certificate file '{customCertsFilePath}' is empty.");
        }

        // Load custom certificates
        var customCerts = new X509Certificate2Collection();
        try
        {
            customCerts.Import(customCertsFilePath);
        }
        catch (Exception ex)
        {
            throw new Exception($"Failed to import custom certificates from '{customCertsFilePath}': {ex.Message}", ex);
        }
        return (_, certificate, chain, sslErrors) =>
        {
            Trace.TraceWarning($"### DEBUG-1: certificate={certificate}"); // TODO simon: rollback!!!
            Console.Out.WriteLine($"### DEBUG-1: certificate={certificate}"); // TODO simon: rollback!!!
            Trace.TraceWarning($"### DEBUG-2: sslErrors={sslErrors}"); // TODO simon: rollback!!!
            Console.Out.WriteLine($"### DEBUG-2: sslErrors={sslErrors}"); // TODO simon: rollback!!!
            if (sslErrors == SslPolicyErrors.None)
            {
                // No errors, certificate is valid
                return true;
            }

            if ((sslErrors & ~SslPolicyErrors.RemoteCertificateChainErrors) != 0)
            {
                // Certificate is not valid due to RemoteCertificateNotAvailable or RemoteCertificateNameMismatch
                return false;
            }

            if (certificate == null || chain == null)
            {
                // Certificate missing
                return false;
            }

            // Certificate validation failed due to chain errors, revalidate the certificate with custom certificates.
            var newChain = new X509Chain();
            newChain.ChainPolicy.ExtraStore.AddRange(customCerts);
            var isValid = newChain.Build(certificate);
            if (isValid) return true;

            // Collect relevant error statuses.
            var hasSelfSignedRoot = IsRootCertificateSelfSigned(newChain);
            var errorStatuses = GetFilteredChainStatuses(newChain, (element, status) =>
            {
                // Ignore UntrustedRoot errors for root certificates from the user-provided custom certificates file.
                // These certificates are explicitly trusted by the user.
                if (status.Status == X509ChainStatusFlags.UntrustedRoot &&
                    ContainsCertificateWithThumbprint(customCerts, element.Certificate.Thumbprint))
                    return false;

                // Ignore RevocationStatusUnknown errors for certificates with self-signed roots.
                // Self-signed certificates typically don't publish revocation information.
                if (status.Status == X509ChainStatusFlags.RevocationStatusUnknown && hasSelfSignedRoot)
                {
                    return false;
                }

                // Ignore NoError statuses.
                return status.Status != X509ChainStatusFlags.NoError;
            });
            if (errorStatuses.Count == 0) return true;

            // Log certificate validation errors and then return false.
            foreach (var status in errorStatuses)
            {
                Trace.TraceWarning($"Certificate chain validation failed: {status.Status}: {status.StatusInformation}");
                Console.Out.WriteLine($"Certificate chain validation failed: {status.Status}: {status.StatusInformation}"); // TODO simon: rollback!!!
            }

            return false;
        };
    }

    private static List<X509ChainStatus> GetFilteredChainStatuses(X509Chain chain,
        Func<X509ChainElement, X509ChainStatus, bool> filter)
    {
        var filtered = new List<X509ChainStatus>();
        foreach (var element in chain.ChainElements)
        {
            if (element.ChainElementStatus == null) continue;
            foreach (var status in element.ChainElementStatus)
            {
                if (filter(element, status))
                {
                    filtered.Add(status);
                }
            }
        }

        return filtered;
    }

    private static bool ContainsCertificateWithThumbprint(X509Certificate2Collection certificates,
        string? certificateThumbprint)
    {
        if (string.IsNullOrEmpty(certificateThumbprint))
        {
            return false;
        }

        foreach (var certificate in certificates)
        {
            if (certificate.Thumbprint != null &&
                certificate.Thumbprint.Equals(certificateThumbprint, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    private static bool IsRootCertificateSelfSigned(X509Chain chain)
    {
        if (chain.ChainElements == null || chain.ChainElements.Count == 0)
        {
            return false;
        }

        // The last certificate should be the root certificate
        var rootCertificate = chain.ChainElements[chain.ChainElements.Count - 1].Certificate;

        return rootCertificate != null && rootCertificate.Issuer == rootCertificate.Subject;
    }
}