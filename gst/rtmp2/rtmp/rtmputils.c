/* GStreamer RTMP Library
 * Copyright (C) 2013 David Schleef <ds@schleef.org>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Library General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this library; if not, write to the
 * Free Software Foundation, Inc., 51 Franklin Street, Suite 500,
 * Boston, MA 02110-1335, USA.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <gst/gst.h>

#include "rtmputils.h"

#include <string.h>


void
gst_rtmp_dump_data (GBytes * bytes)
{
  const guint8 *data;
  gsize size;
  int i, j;

  data = g_bytes_get_data (bytes, &size);
  for (i = 0; i < (int) size; i += 16) {
    g_print ("%04x: ", i);
    for (j = 0; j < 16; j++) {
      if (i + j < (int) size) {
        g_print ("%02x ", data[i + j]);
      } else {
        g_print ("   ");
      }
    }
    for (j = 0; j < 16; j++) {
      if (i + j < (int) size) {
        g_print ("%c", g_ascii_isprint (data[i + j]) ? data[i + j] : '.');
      }
    }
    g_print ("\n");
  }
}

GBytes *
gst_rtmp_bytes_remove (GBytes * bytes, gsize size)
{
  GBytes *new_bytes;

  new_bytes =
      g_bytes_new_from_bytes (bytes, size, g_bytes_get_size (bytes) - size);
  g_bytes_unref (bytes);

  return new_bytes;
}

gchar *
gst_rtmp_hexify (const guint8 * src, gsize size)
{
  static const char xdigit[] = "0123456789abcdef";
  int i;
  gchar *dest;
  dest = g_malloc (2 * size + 1);
  for (i = 0; i < (int) size; i++) {
    dest[2 * i] = xdigit[src[i] >> 4];
    dest[2 * i + 1] = xdigit[src[i] & 0xf];
  }
  dest[2 * size] = 0;
  return dest;
}

guint8 *
gst_rtmp_unhexify (const char *src, gsize * size)
{
  int i, n;
  guint8 *dest;
  n = strlen (src) / 2;
  dest = g_malloc (n + 1);
  for (i = 0; i < n; i++) {
    dest[i] = g_ascii_xdigit_value (src[2 * i]) << 4 |
        g_ascii_xdigit_value (src[2 * i + 1]);
  }
  dest[n] = 0;
  if (size)
    *size = n;
  return dest;
}

/* taken from librtmp */
gchar *
gst_rtmp_tea_decode (const gchar * key, const gchar * text)
{
  guint32 *v, k[4] = { 0 }, u;
  guint32 z, y, sum = 0, e, DELTA = 0x9e3779b9;
  gint32 p, q;
  int i, n;
  unsigned char *ptr, *out;

  /* prep key: pack 1st 16 chars into 4 LittleEndian ints */
  ptr = (unsigned char *) key;
  u = 0;
  n = 0;
  v = k;
  p = strlen (key) > 16 ? 16 : strlen (key);
  for (i = 0; i < p; i++) {
    u |= ptr[i] << (n * 8);
    if (n == 3) {
      *v++ = u;
      u = 0;
      n = 0;
    } else {
      n++;
    }
  }
  /* any trailing chars */
  if (u)
    *v = u;

  /* prep text: hex2bin, multiples of 4 */
  n = (strlen (text) + 7) / 8;
  out = malloc (n * 8);
  ptr = (unsigned char *) text;
  v = (guint32 *) out;
  for (i = 0; i < n; i++) {
#define HEX2BIN(x) g_ascii_xdigit_value(x)
    u = (HEX2BIN (ptr[0]) << 4) + HEX2BIN (ptr[1]);
    u |= ((HEX2BIN (ptr[2]) << 4) + HEX2BIN (ptr[3])) << 8;
    u |= ((HEX2BIN (ptr[4]) << 4) + HEX2BIN (ptr[5])) << 16;
    u |= ((HEX2BIN (ptr[6]) << 4) + HEX2BIN (ptr[7])) << 24;
    *v++ = u;
    ptr += 8;
  }
  v = (guint32 *) out;

  /* http://www.movable-type.co.uk/scripts/tea-block.html */
#define MX (((z>>5)^(y<<2)) + ((y>>3)^(z<<4))) ^ ((sum^y) + (k[(p&3)^e]^z));
  z = v[n - 1];
  y = v[0];
  q = 6 + 52 / n;
  sum = q * DELTA;
  while (sum != 0) {
    e = sum >> 2 & 3;
    for (p = n - 1; p > 0; p--)
      z = v[p - 1], y = v[p] -= MX;
    z = v[n - 1];
    y = v[0] -= MX;
    sum -= DELTA;
  }

  return (gchar *) out;
}

static void
dump_command (GstRtmpChunk * chunk)
{
  GstAmfNode *amf;
  gsize size;
  const guint8 *data;
  gsize n_parsed;
  int offset;

  offset = 0;
  data = g_bytes_get_data (chunk->payload, &size);
  while (offset < (int) size) {
    amf = gst_amf_node_new_parse (data + offset, size - offset, &n_parsed);
    gst_amf_node_dump (amf);
    gst_amf_node_free (amf);
    offset += n_parsed;
  }
}

void
gst_rtmp_dump_chunk (GstRtmpChunk * chunk, gboolean dir, gboolean dump_message,
    gboolean dump_data)
{
  g_print ("%s chunk_stream_id:%-4d ts:%-8d len:%-6" G_GSIZE_FORMAT
      " type_id:%-4d stream_id:%08x\n", dir ? ">>>" : "<<<",
      chunk->chunk_stream_id,
      chunk->timestamp,
      chunk->message_length, chunk->message_type_id, chunk->stream_id);
  if (dump_message) {
    if (chunk->message_type_id == GST_RTMP_MESSAGE_TYPE_COMMAND ||
        chunk->message_type_id == GST_RTMP_MESSAGE_TYPE_DATA) {
      dump_command (chunk);
    }
  }
  if (dump_data) {
    gst_rtmp_dump_data (chunk->payload);
  }
}
