using System;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using System.Text;

namespace InfluxDB3.Client.Internal;

internal class GzipHandler
{
    private readonly int _threshold;

    public GzipHandler(int threshold)
    {
        _threshold = threshold;
    }

    public HttpContent? Process(string body)
    {
        if (_threshold > 0 && body.Length < _threshold)
        {
            return null;
        }

        using (var msi = new MemoryStream(Encoding.UTF8.GetBytes(body)))
        using (var mso = new MemoryStream())
        {
            using (var gs = new GZipStream(mso, CompressionMode.Compress))
            {
                msi.CopyTo(gs);
                gs.Flush();
            }

            var content = new ByteArrayContent(mso.ToArray());
            content.Headers.Add("Content-Type", "text/plain; charset=utf-8");
            content.Headers.Add("Content-Encoding", "gzip");
            return content;
        }
    }
}
