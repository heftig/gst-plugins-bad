/* GStreamer RTMP Library
 * Copyright (C) 2013 David Schleef <ds@schleef.org>
 * Copyright (C) 2017 Make.TV, Inc. <info@make.tv>
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
#include <gio/gio.h>
#include <string.h>
#include "rtmpclient.h"
#include "rtmphandshake.h"
#include "rtmputils.h"

GST_DEBUG_CATEGORY_STATIC (gst_rtmp_client_debug_category);
#define GST_CAT_DEFAULT gst_rtmp_client_debug_category

static void socket_connect (GTask * task);
static void socket_connect_done (GObject * source, GAsyncResult * result,
    gpointer user_data);
static void handshake_done (GObject * source, GAsyncResult * result,
    gpointer user_data);
static void send_connect (GTask * task);
static void send_connect_done (const gchar * command_name, GPtrArray * args,
    gpointer user_data);
static void send_secure_token_response (GTask * task,
    GstRtmpConnection * connection, const gchar * challenge);
static void connection_error (GstRtmpConnection * connection,
    gpointer user_data);

void
gst_rtmp_location_copy (GstRtmpLocation * dest, const GstRtmpLocation * src)
{
  g_return_if_fail (dest);
  g_return_if_fail (src);

  dest->host = g_strdup (src->host);
  dest->port = src->port;
  dest->application = g_strdup (src->application);
  dest->stream = g_strdup (src->stream);
  dest->username = g_strdup (src->username);
  dest->password = g_strdup (src->password);
  dest->secure_token = g_strdup (src->secure_token);
  dest->authmod = src->authmod;
  dest->timeout = src->timeout;
}

void
gst_rtmp_location_clear (GstRtmpLocation * location)
{
  g_return_if_fail (location);

  g_clear_pointer (&location->host, g_free);
  location->port = 0;
  g_clear_pointer (&location->application, g_free);
  g_clear_pointer (&location->stream, g_free);
  g_clear_pointer (&location->username, g_free);
  g_clear_pointer (&location->password, g_free);
  g_clear_pointer (&location->secure_token, g_free);
}

gchar *
gst_rtmp_location_get_string (const GstRtmpLocation * uri, gboolean with_stream)
{
  GstUri *gsturi;
  gchar *string;

  gsturi = gst_uri_new ("rtmp", NULL, uri->host,
      uri->port == GST_RTMP_DEFAULT_PORT ? GST_URI_NO_PORT : uri->port, "/",
      NULL, NULL);

  gst_uri_append_path_segment (gsturi, uri->application);

  if (with_stream) {
    gst_uri_append_path_segment (gsturi, uri->stream);
  }

  string = gst_uri_to_string (gsturi);
  gst_uri_unref (gsturi);

  return string;
}

#define DEFAULT_TIMEOUT 5

typedef struct
{
  GstRtmpLocation location;
  gchar *auth_query;
  GstRtmpConnection *connection;
  gulong error_handler_id;
} ConnectTaskData;

static ConnectTaskData *
connect_task_data_new (const GstRtmpLocation * location)
{
  ConnectTaskData *data = g_slice_new0 (ConnectTaskData);
  gst_rtmp_location_copy (&data->location, location);
  return data;
}

static void
connect_task_data_free (gpointer ptr)
{
  ConnectTaskData *data = ptr;
  gst_rtmp_location_clear (&data->location);
  g_clear_pointer (&data->auth_query, g_free);
  if (data->error_handler_id) {
    g_signal_handler_disconnect (data->connection, data->error_handler_id);
  }
  g_clear_object (&data->connection);
  g_slice_free (ConnectTaskData, data);
}

static GRegex *auth_regex = NULL;

GType
gst_rtmp_authmod_get_type (void)
{
  static volatile gsize authmod_type = 0;
  static const GEnumValue authmod[] = {
    {GST_RTMP_AUTHMOD_NONE, "GST_RTMP_AUTHMOD_NONE", "none"},
    {GST_RTMP_AUTHMOD_AUTO, "GST_RTMP_AUTHMOD_AUTO", "auto"},
    {GST_RTMP_AUTHMOD_ADOBE, "GST_RTMP_AUTHMOD_ADOBE", "adobe"},
    {0, NULL, NULL},
  };

  if (g_once_init_enter (&authmod_type)) {
    GType tmp = g_enum_register_static ("GstRtmpAuthmod", authmod);
    g_once_init_leave (&authmod_type, tmp);
  }

  return (GType) authmod_type;
}

static const gchar *
gst_rtmp_authmod_get_nick (GstRtmpAuthmod value)
{
  GEnumClass *klass = g_type_class_peek (GST_TYPE_RTMP_AUTHMOD);
  GEnumValue *ev = klass ? g_enum_get_value (klass, value) : NULL;
  return ev ? ev->value_nick : "(unknown)";
}

void
gst_rtmp_client_connect_async (const GstRtmpLocation * location,
    GCancellable * cancellable, GAsyncReadyCallback callback,
    gpointer user_data)
{
  GTask *task;

  if (g_once_init_enter (&auth_regex)) {
    GRegex *re = g_regex_new ("\\[ *AccessManager.Reject *\\] *: *"
        "\\[ *authmod=(?<authmod>.*?) *\\] *: *"
        "(?<query>\\?.*)\\Z", G_REGEX_DOTALL, 0, NULL);
    g_warn_if_fail (re);

    GST_DEBUG_CATEGORY_INIT (gst_rtmp_client_debug_category,
        "rtmpclient", 0, "debug category for rtmpclient");

    g_once_init_leave (&auth_regex, re);
  }

  task = g_task_new (NULL, cancellable, callback, user_data);

  g_task_set_task_data (task, connect_task_data_new (location),
      connect_task_data_free);

  socket_connect (task);
}

static void
socket_connect (GTask * task)
{
  ConnectTaskData *data = g_task_get_task_data (task);
  GSocketConnectable *addr;
  GSocketClient *socket_client;

  if (data->location.timeout < 0) {
    data->location.timeout = DEFAULT_TIMEOUT;
  }

  if (data->error_handler_id) {
    g_signal_handler_disconnect (data->connection, data->error_handler_id);
    data->error_handler_id = 0;
  }

  if (data->connection) {
    gst_rtmp_connection_close (data->connection);
    g_clear_object (&data->connection);
  }

  addr = g_network_address_new (data->location.host, data->location.port);
  socket_client = g_socket_client_new ();
  g_socket_client_set_timeout (socket_client, data->location.timeout);

  GST_DEBUG ("Starting socket connection");

  g_socket_client_connect_async (socket_client, addr,
      g_task_get_cancellable (task), socket_connect_done, task);
  g_object_unref (addr);
  g_object_unref (socket_client);
}

static void
socket_connect_done (GObject * source, GAsyncResult * result,
    gpointer user_data)
{
  GSocketClient *socket_client = G_SOCKET_CLIENT (source);
  GSocketConnection *socket_connection;
  GTask *task = user_data;
  GError *error = NULL;

  socket_connection =
      g_socket_client_connect_finish (socket_client, result, &error);

  if (g_task_return_error_if_cancelled (task)) {
    GST_DEBUG ("Socket connection was cancelled");
    g_object_unref (task);
    return;
  }

  if (socket_connection == NULL) {
    GST_ERROR ("Socket connection error");
    g_task_return_error (task, error);
    g_object_unref (task);
    return;
  }

  GST_DEBUG ("Socket connection established");

  gst_rtmp_client_handshake (G_IO_STREAM (socket_connection),
      g_task_get_cancellable (task), handshake_done, task);
  g_object_unref (socket_connection);
}


static void
handshake_done (GObject * source, GAsyncResult * result, gpointer user_data)
{
  GIOStream *stream = G_IO_STREAM (source);
  GSocketConnection *socket_connection = G_SOCKET_CONNECTION (stream);
  GTask *task = user_data;
  ConnectTaskData *data = g_task_get_task_data (task);
  GError *error = NULL;
  gboolean res;

  res = gst_rtmp_client_handshake_finish (stream, result, &error);
  if (!res) {
    g_io_stream_close_async (stream, G_PRIORITY_DEFAULT, NULL, NULL, NULL);
    g_task_return_error (task, error);
    g_object_unref (task);
    return;
  }

  data->connection = gst_rtmp_connection_new (socket_connection);
  data->error_handler_id = g_signal_connect (data->connection,
      "error", G_CALLBACK (connection_error), task);

  send_connect (task);
}

static void
connection_error (GstRtmpConnection * connection, gpointer user_data)
{
  GTask *task = user_data;
  g_task_return_new_error (task, G_IO_ERROR, G_IO_ERROR_FAILED,
      "error during connection attempt");
}

static gchar *
do_adobe_auth (const gchar * username, const gchar * password,
    const gchar * salt, const gchar * opaque, const gchar * challenge)
{
  guint8 hash[16];              /* MD5 digest */
  gsize hashlen = sizeof hash;
  gchar *challenge2, *auth_query;
  GChecksum *md5;

  md5 = g_checksum_new (G_CHECKSUM_MD5);
  g_checksum_update (md5, (guchar *) username, -1);
  g_checksum_update (md5, (guchar *) salt, -1);
  g_checksum_update (md5, (guchar *) password, -1);

  g_checksum_get_digest (md5, hash, &hashlen);
  g_warn_if_fail (hashlen == sizeof hash);

  {
    gchar *hashstr = g_base64_encode ((guchar *) hash, sizeof hash);
    g_checksum_reset (md5);
    g_checksum_update (md5, (guchar *) hashstr, -1);
    g_free (hashstr);
  }

  if (opaque)
    g_checksum_update (md5, (guchar *) opaque, -1);
  else if (challenge)
    g_checksum_update (md5, (guchar *) challenge, -1);

  challenge2 = g_strdup_printf ("%08x", g_random_int ());
  g_checksum_update (md5, (guchar *) challenge2, -1);

  g_checksum_get_digest (md5, hash, &hashlen);
  g_warn_if_fail (hashlen == sizeof hash);

  {
    gchar *hashstr = g_base64_encode ((guchar *) hash, sizeof hash);

    if (opaque) {
      auth_query =
          g_strdup_printf
          ("authmod=%s&user=%s&challenge=%s&response=%s&opaque=%s", "adobe",
          username, challenge2, hashstr, opaque);
    } else {
      auth_query =
          g_strdup_printf ("authmod=%s&user=%s&challenge=%s&response=%s",
          "adobe", username, challenge2, hashstr);
    }
    g_free (hashstr);
  }

  g_checksum_free (md5);
  g_free (challenge2);

  return auth_query;
}

static void
send_connect (GTask * task)
{
  ConnectTaskData *data = g_task_get_task_data (task);
  GstAmfNode *node;
  const gchar *app;
  gchar *uri, *appstr = NULL, *uristr = NULL;

  node = gst_amf_node_new_object ();
  app = data->location.application;
  uri = gst_rtmp_location_get_string (&data->location, FALSE);

  if (data->auth_query) {
    const gchar *query = data->auth_query;
    appstr = g_strdup_printf ("%s?%s", app, query);
    uristr = g_strdup_printf ("%s?%s", uri, query);
  } else if (data->location.authmod == GST_RTMP_AUTHMOD_ADOBE) {
    const gchar *user = data->location.username;
    const gchar *authmod = "adobe";
    appstr = g_strdup_printf ("%s?authmod=%s&user=%s", app, authmod, user);
    uristr = g_strdup_printf ("%s?authmod=%s&user=%s", uri, authmod, user);
  } else {
    appstr = g_strdup (app);
    uristr = g_strdup (uri);
  }

  gst_amf_node_append_field_take_string (node, g_strdup ("app"), appstr);
  gst_amf_node_append_field_take_string (node, g_strdup ("tcUrl"), uristr);
  gst_amf_node_append_field_string (node, "type", "nonprivate");
  gst_amf_node_append_field_string (node, "flashVer", "FMLE/3.0");

  gst_rtmp_connection_send_command (data->connection, send_connect_done,
      task, 0, "connect", node, NULL);

  gst_amf_node_free (node);
  g_free (uri);
}

static void
send_connect_done (const gchar * command_name, GPtrArray * args,
    gpointer user_data)
{
  GTask *task = G_TASK (user_data);
  ConnectTaskData *data = g_task_get_task_data (task);
  const GstAmfNode *node, *optional_args;
  const gchar *code;

  if (g_task_return_error_if_cancelled (task)) {
    g_object_unref (task);
    return;
  }

  if (!args || args->len < 2) {
    g_task_return_new_error (task, G_IO_ERROR, G_IO_ERROR_FAILED,
        "connect failed");
    g_object_unref (task);
    return;
  }

  optional_args = g_ptr_array_index (args, 1);

  node = gst_amf_node_get_field (optional_args, "code");
  if (!node) {
    g_task_return_new_error (task, G_IO_ERROR, G_IO_ERROR_FAILED,
        "result code missing from connect cmd result");
    g_object_unref (task);
    return;
  }

  code = gst_amf_node_peek_string (node);
  GST_INFO ("connect result: %s", GST_STR_NULL (code));

  if (g_str_equal (code, "NetConnection.Connect.Success")) {
    node = gst_amf_node_get_field (optional_args, "secureToken");
    send_secure_token_response (task, data->connection,
        node ? gst_amf_node_peek_string (node) : NULL);
    return;
  }

  if (g_str_equal (code, "NetConnection.Connect.Rejected")) {
    GstRtmpAuthmod authmod = data->location.authmod;
    GMatchInfo *match_info;
    const gchar *desc;
    GstUri *query;

    node = gst_amf_node_get_field (optional_args, "description");
    if (!node) {
      g_task_return_new_error (task, G_IO_ERROR, G_IO_ERROR_PERMISSION_DENIED,
          "Connect rejected; no description");
      g_object_unref (task);
      return;
    }

    desc = gst_amf_node_peek_string (node);
    GST_DEBUG ("connect result desc: %s", GST_STR_NULL (desc));

    if (authmod == GST_RTMP_AUTHMOD_AUTO && strstr (desc, "code=403 need auth")) {
      if (strstr (desc, "authmod=adobe")) {
        data->location.authmod = GST_RTMP_AUTHMOD_ADOBE;
        socket_connect (task);
        return;
      }

      g_task_return_new_error (task, G_IO_ERROR, G_IO_ERROR_PERMISSION_DENIED,
          "unhandled authentication mode: %s", desc);
      g_object_unref (task);
      return;
    }

    if (!g_regex_match (auth_regex, desc, 0, &match_info)) {
      g_task_return_new_error (task, G_IO_ERROR, G_IO_ERROR_PERMISSION_DENIED,
          "failed to parse auth rejection: %s", desc);
      g_object_unref (task);
      return;
    }

    {
      gchar *authmod_str = g_match_info_fetch_named (match_info, "authmod");
      gchar *query_str = g_match_info_fetch_named (match_info, "query");
      gboolean matches;

      GST_INFO ("regex parsed auth: authmod=%s, query=%s",
          GST_STR_NULL (authmod_str), GST_STR_NULL (query_str));
      g_match_info_free (match_info);

      switch (authmod) {
        case GST_RTMP_AUTHMOD_ADOBE:
          matches = g_str_equal (authmod_str, "adobe");
          break;

        default:
          matches = FALSE;
          break;
      }

      if (!matches) {
        g_task_return_new_error (task, G_IO_ERROR, G_IO_ERROR_PERMISSION_DENIED,
            "server uses wrong authentication mode '%s'; expected %s",
            GST_STR_NULL (authmod_str), gst_rtmp_authmod_get_nick (authmod));
        g_object_unref (task);
        g_free (authmod_str);
        g_free (query_str);
        return;
      }
      g_free (authmod_str);

      query = gst_uri_from_string (query_str);
      if (!query) {
        g_task_return_new_error (task, G_IO_ERROR, G_IO_ERROR_PERMISSION_DENIED,
            "failed to parse authentication query '%s'",
            GST_STR_NULL (query_str));
        g_object_unref (task);
        g_free (query_str);
        return;
      }
      g_free (query_str);
    }

    {
      const gchar *reason = gst_uri_get_query_value (query, "reason");

      if (g_str_equal (reason, "authfailed")) {
        g_task_return_new_error (task, G_IO_ERROR, G_IO_ERROR_PERMISSION_DENIED,
            "authentication failed! wrong credentials?");
        g_object_unref (task);
        gst_uri_unref (query);
        return;
      }

      if (!g_str_equal (reason, "needauth")) {
        g_task_return_new_error (task, G_IO_ERROR, G_IO_ERROR_PERMISSION_DENIED,
            "unhandled rejection reason '%s'", reason ? reason : "");
        g_object_unref (task);
        gst_uri_unref (query);
        return;
      }
    }

    g_warn_if_fail (!data->auth_query);
    data->auth_query = do_adobe_auth (data->location.username,
        data->location.password, gst_uri_get_query_value (query, "salt"),
        gst_uri_get_query_value (query, "opaque"),
        gst_uri_get_query_value (query, "challenge"));

    gst_uri_unref (query);

    if (!data->auth_query) {
      g_task_return_new_error (task, G_IO_ERROR, G_IO_ERROR_FAILED,
          "couldn't generate adobe style authentication query");
      g_object_unref (task);
      return;
    }

    socket_connect (task);
    return;
  }

  g_task_return_new_error (task, G_IO_ERROR, G_IO_ERROR_FAILED,
      "unhandled connect result code: %s", GST_STR_NULL (code));
  g_object_unref (task);
}

/* taken from librtmp */
static gchar *
rtmp_tea_decode (const gchar * key, const gchar * text)
{
  guint32 *v, k[4] = { 0 }, u;
  guint32 z, y, sum = 0, e, DELTA = 0x9e3779b9;
  gint32 p, q;
  gint i, n;
  guchar *ptr, *out;

  /* prep key: pack 1st 16 chars into 4 LittleEndian ints */
  ptr = (guchar *) key;
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
  out = g_malloc (n * 8);
  ptr = (guchar *) text;
  v = (guint32 *) out;
  for (i = 0; i < n; i++) {
    u = (g_ascii_xdigit_value (ptr[0]) << 4) + g_ascii_xdigit_value (ptr[1]);
    u |= ((g_ascii_xdigit_value (ptr[2]) << 4) +
        g_ascii_xdigit_value (ptr[3])) << 8;
    u |= ((g_ascii_xdigit_value (ptr[4]) << 4) +
        g_ascii_xdigit_value (ptr[5])) << 16;
    u |= ((g_ascii_xdigit_value (ptr[6]) << 4) +
        g_ascii_xdigit_value (ptr[7])) << 24;
    *v++ = u;
    ptr += 8;
  }
  v = (guint32 *) out;

  /* http://www.movable-type.co.uk/scripts/tea-block.html */
  z = v[n - 1];
  y = v[0];
  q = 6 + 52 / n;
  sum = q * DELTA;
  while (sum != 0) {
    e = sum >> 2 & 3;
    for (p = n - 1; p > 0; p--)
      z = v[p - 1], y = v[p] -=
          (((z >> 5) ^ (y << 2)) + ((y >> 3) ^ (z << 4))) ^ ((sum ^ y) +
          (k[(p & 3) ^ e] ^ z));
    z = v[n - 1];
    y = v[0] -=
        (((z >> 5) ^ (y << 2)) + ((y >> 3) ^ (z << 4))) ^ ((sum ^ y) +
        (k[(p & 3) ^ e] ^ z));
    sum -= DELTA;
  }

  return (gchar *) out;
}

static void
send_secure_token_response (GTask * task, GstRtmpConnection * connection,
    const gchar * challenge)
{
  ConnectTaskData *data = g_task_get_task_data (task);
  if (challenge) {
    GstAmfNode *node1;
    GstAmfNode *node2;
    gchar *response;

    if (!data->location.secure_token || !data->location.secure_token[0]) {
      g_task_return_new_error (task, G_IO_ERROR, G_IO_ERROR_PERMISSION_DENIED,
          "server requires secure token authentication");
      g_object_unref (task);
      return;
    }

    response = rtmp_tea_decode (data->location.secure_token, challenge);

    GST_DEBUG ("response: %s", response);

    node1 = gst_amf_node_new_null ();
    node2 = gst_amf_node_new_take_string (response);
    gst_rtmp_connection_send_command (connection, NULL, NULL, 0,
        "secureTokenResponse", node1, node2, NULL);
    gst_amf_node_free (node1);
    gst_amf_node_free (node2);
  }

  g_signal_handler_disconnect (connection, data->error_handler_id);
  data->error_handler_id = 0;

  g_task_return_pointer (task, g_object_ref (connection),
      gst_rtmp_connection_close_and_unref);
  g_object_unref (task);
}

GstRtmpConnection *
gst_rtmp_client_connect_finish (GAsyncResult * result, GError ** error)
{
  GTask *task = G_TASK (result);
  return g_task_propagate_pointer (task, error);
}
