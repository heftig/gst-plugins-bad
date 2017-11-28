/* GStreamer
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
 * Free Software Foundation, Inc., 51 Franklin St, Fifth Floor,
 * Boston, MA 02110-1301, USA.
 */

#include "gstrtmp2locationhandler.h"
#include "rtmp/rtmputils.h"
#include "rtmp/rtmpclient.h"

#define DEFAULT_SCHEME GST_RTMP_SCHEME_RTMP
#define DEFAULT_HOST "localhost"
#define DEFAULT_APPLICATION "live"
#define DEFAULT_STREAM "myStream"
#define DEFAULT_LOCATION "rtmp://" DEFAULT_HOST "/" DEFAULT_APPLICATION "/" DEFAULT_STREAM
#define DEFAULT_SECURE_TOKEN NULL
#define DEFAULT_USERNAME NULL
#define DEFAULT_PASSWORD NULL
#define DEFAULT_AUTHMOD  GST_RTMP_AUTHMOD_AUTO
#define DEFAULT_TIMEOUT 5

G_DEFINE_INTERFACE (GstRtmpLocationHandler, gst_rtmp_location_handler, 0);

#define GST_CAT_DEFAULT gst_rtmp_location_handler_debug_category
GST_DEBUG_CATEGORY_STATIC (GST_CAT_DEFAULT);

static void
gst_rtmp_location_handler_default_init (GstRtmpLocationHandlerInterface * iface)
{
  GST_DEBUG_CATEGORY_INIT (GST_CAT_DEFAULT, "rtmp2urihandler", 0,
      "RTMP2 URI Handling");

  g_object_interface_install_property (iface, g_param_spec_string ("location",
          "Location", "Location of RTMP stream to access", DEFAULT_LOCATION,
          G_PARAM_CONSTRUCT | G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_interface_install_property (iface, g_param_spec_enum ("scheme",
          "Scheme", "RTMP connection scheme",
          GST_TYPE_RTMP_SCHEME, DEFAULT_SCHEME,
          G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_interface_install_property (iface, g_param_spec_string ("host",
          "Host", "RTMP server host name", DEFAULT_HOST,
          G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_interface_install_property (iface, g_param_spec_int ("port", "Port",
          "RTMP server port", 1, 65535,
          gst_rtmp_scheme_get_default_port (DEFAULT_SCHEME),
          G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_interface_install_property (iface,
      g_param_spec_string ("application", "Application",
          "RTMP application path", DEFAULT_APPLICATION,
          G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_interface_install_property (iface, g_param_spec_string ("stream",
          "Stream", "RTMP stream path", DEFAULT_STREAM,
          G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_interface_install_property (iface, g_param_spec_string ("username",
          "User name", "RTMP authorization user name", DEFAULT_USERNAME,
          G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_interface_install_property (iface, g_param_spec_string ("password",
          "Password", "RTMP authorization password", DEFAULT_PASSWORD,
          G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_interface_install_property (iface,
      g_param_spec_string ("secure-token", "Secure token",
          "RTMP authorization token", DEFAULT_SECURE_TOKEN,
          G_PARAM_CONSTRUCT | G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_interface_install_property (iface, g_param_spec_enum ("authmod",
          "Authorization mode", "RTMP authorization mode",
          GST_TYPE_RTMP_AUTHMOD, DEFAULT_AUTHMOD,
          G_PARAM_CONSTRUCT | G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_interface_install_property (iface, g_param_spec_uint ("timeout",
          "Timeout", "RTMP timeout in seconds", 0, G_MAXUINT, DEFAULT_TIMEOUT,
          G_PARAM_CONSTRUCT | G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_interface_install_property (iface,
      g_param_spec_flags ("tls-validation-flags", "TLS validation flags",
          "TLS validation flags to use", G_TYPE_TLS_CERTIFICATE_FLAGS,
          G_TLS_CERTIFICATE_VALIDATE_ALL,
          G_PARAM_CONSTRUCT | G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
}

static GstURIType
uri_handler_get_type_sink (GType type)
{
  return GST_URI_SINK;
}

static GstURIType
uri_handler_get_type_src (GType type)
{
  return GST_URI_SRC;
}

static const gchar *const *
uri_handler_get_protocols (GType type)
{
  return gst_rtmp_scheme_get_strings ();
}

static gchar *
uri_handler_get_uri (GstURIHandler * handler)
{
  GstRtmpLocation location = { 0, };
  gchar *string;

  g_object_get (handler, "scheme", &location.scheme, "host", &location.host,
      "port", &location.port, "application", &location.application,
      "stream", &location.stream, NULL);

  string = gst_rtmp_location_get_string (&location, TRUE);
  gst_rtmp_location_clear (&location);
  return string;
}

static gboolean
parse_path (const gchar * string, GstUri * uri, gchar ** application,
    gchar ** stream, GError ** error)
{
  GList *segments;
  guint nsegments;

  g_return_val_if_fail (uri, FALSE);
  g_return_val_if_fail (application, FALSE);
  g_return_val_if_fail (stream, FALSE);

  segments = gst_uri_get_path_segments (uri);
  nsegments = g_list_length (segments);

  /* Test if too short, or not absolute */
  if (nsegments < 3) {
    g_set_error (error, GST_URI_ERROR, GST_URI_ERROR_BAD_REFERENCE,
        "URI path too short: %s", string);
    goto err;
  }

  if (segments->data != NULL) {
    g_set_error (error, GST_URI_ERROR, GST_URI_ERROR_BAD_REFERENCE,
        "URI path not absolute: %s", string);
    goto err;
  }

  /* Strip root */
  segments = g_list_delete_link (segments, segments);

  /* Extract stream */
  {
    gchar *query = gst_uri_get_query_string (uri);
    GList *segment = g_list_last (segments);
    gchar *streamname = segment->data;

    if (query && query[0]) {
      *stream = g_strconcat (streamname, "?", query, NULL);
      g_free (streamname);
    } else {
      *stream = streamname;
    }

    segments = g_list_delete_link (segments, segment);
    g_free (query);
  }

  {
    GstUri *tempuri =
        gst_uri_new (NULL, NULL, NULL, GST_URI_NO_PORT, NULL, NULL, NULL);

    gst_uri_set_path_segments (tempuri, segments);

    *application = gst_uri_get_path (tempuri);
    gst_uri_unref (tempuri);
  }

  return TRUE;

err:
  g_list_free_full (segments, g_free);
  return FALSE;
}

static gboolean
uri_handler_set_uri (GstURIHandler * handler, const gchar * string,
    GError ** error)
{
  GstRtmpLocationHandler *self = GST_RTMP_LOCATION_HANDLER (handler);
  GstUri *uri;
  const gchar *scheme_string, *host, *userinfo;
  GstRtmpScheme scheme;
  gchar *application, *stream;
  guint port;
  gboolean ret = FALSE;

  GST_DEBUG_OBJECT (self, "setting URI to %s", GST_STR_NULL (string));

  uri = gst_uri_from_string (string);
  if (!uri) {
    g_set_error (error, GST_URI_ERROR, GST_URI_ERROR_BAD_URI,
        "URI failed to parse: %s", GST_STR_NULL (string));
    return FALSE;
  }

  gst_uri_normalize (uri);

  scheme_string = gst_uri_get_scheme (uri);
  if (!scheme_string) {
    g_set_error (error, GST_URI_ERROR, GST_URI_ERROR_BAD_REFERENCE,
        "URI lacks scheme: %s", string);
    goto out;
  }

  scheme = gst_rtmp_scheme_from_string (scheme_string);
  if (scheme < 0) {
    g_set_error (error, GST_URI_ERROR, GST_URI_ERROR_BAD_REFERENCE,
        "URI has bad scheme '%s': %s", scheme_string, string);
    goto out;
  }

  host = gst_uri_get_host (uri);
  if (!host) {
    g_set_error (error, GST_URI_ERROR, GST_URI_ERROR_BAD_REFERENCE,
        "URI lacks hostname: %s", string);
    goto out;
  }

  port = gst_uri_get_port (uri);
  if (port == GST_URI_NO_PORT) {
    port = gst_rtmp_scheme_get_default_port (scheme);
  }

  if (!parse_path (string, uri, &application, &stream, error)) {
    goto out;
  }

  g_object_set (self, "scheme", scheme, "host", host, "port", port,
      "application", application, "stream", stream, "username", NULL,
      "password", NULL, NULL);

  g_free (application);
  g_free (stream);

  userinfo = gst_uri_get_userinfo (uri);
  if (userinfo) {
    gchar **split = g_strsplit (userinfo, ":", 2);

    if (!split || !split[0] || !split[1]) {
      g_set_error (error, GST_URI_ERROR, GST_URI_ERROR_BAD_REFERENCE,
          "Failed to parse username:password data");
      g_strfreev (split);
      goto out;
    }

    g_object_set (self, "username", split[0], "password", split[1], NULL);
    g_strfreev (split);
  }

  ret = TRUE;

out:
  gst_uri_unref (uri);
  return ret;
}

void
gst_rtmp_location_handler_implement_uri_handler (GstURIHandlerInterface * iface,
    GstURIType type)
{
  switch (type) {
    case GST_URI_SINK:
      iface->get_type = uri_handler_get_type_sink;
      break;
    case GST_URI_SRC:
      iface->get_type = uri_handler_get_type_src;
      break;
    default:
      g_return_if_reached ();
  }
  iface->get_protocols = uri_handler_get_protocols;
  iface->get_uri = uri_handler_get_uri;
  iface->set_uri = uri_handler_set_uri;
}

gboolean
gst_rtmp_location_handler_set_uri (GstRtmpLocationHandler * handler,
    const gchar * uri)
{
  GError *error = NULL;
  gboolean ret;

  ret = gst_uri_handler_set_uri (GST_URI_HANDLER (handler), uri, &error);
  if (!ret) {
    GST_ERROR_OBJECT (handler, "Failed to set URI: %s", error->message);
    g_error_free (error);
  }
  return ret;
}
