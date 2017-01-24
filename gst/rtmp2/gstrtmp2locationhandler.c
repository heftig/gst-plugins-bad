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

#define DEFAULT_HOST "localhost"
#define DEFAULT_APPLICATION "live"
#define DEFAULT_STREAM "myStream"
#define DEFAULT_LOCATION "rtmp://" DEFAULT_HOST "/" DEFAULT_APPLICATION "/" DEFAULT_STREAM
#define DEFAULT_SECURE_TOKEN ""
#define DEFAULT_USERNAME ""
#define DEFAULT_PASSWORD ""
#define DEFAULT_SECURE_TOKEN ""
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
  g_object_interface_install_property (iface, g_param_spec_string ("host",
          "Host", "RTMP server host name", DEFAULT_HOST,
          G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_interface_install_property (iface, g_param_spec_int ("port", "Port",
          "RTMP server port", 1, 65535, GST_RTMP_DEFAULT_PORT,
          G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_interface_install_property (iface,
      g_param_spec_string ("application", "Application",
          "RTMP application name", DEFAULT_APPLICATION,
          G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_interface_install_property (iface, g_param_spec_string ("stream",
          "Stream", "RTMP stream name", DEFAULT_STREAM,
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
  static const gchar *protocols[] = { "rtmp", NULL };
  return protocols;
}

static gchar *
uri_handler_get_uri (GstURIHandler * handler)
{
  GstRtmpLocation location = { NULL, };
  gchar *string;

  g_object_get (handler, "host", &location.host, "port", &location.port,
      "application", &location.application, "stream", &location.stream, NULL);

  string = gst_rtmp_location_get_string (&location, TRUE);
  gst_rtmp_location_clear (&location);
  return string;
}

static gboolean
uri_handler_set_uri (GstURIHandler * handler, const gchar * string,
    GError ** error)
{
  GstRtmpLocationHandler *self = GST_RTMP_LOCATION_HANDLER (handler);
  GstUri *uri;
  const gchar *host, *userinfo;
  guint port, nsegments;
  GList *segments = NULL;
  gboolean ret = FALSE;

  GST_DEBUG_OBJECT (self, "setting URI to %s", GST_STR_NULL (string));

  uri = gst_uri_from_string (string);
  if (!uri) {
    g_set_error (error, GST_URI_ERROR, GST_URI_ERROR_BAD_URI,
        "URI failed to parse: %s", GST_STR_NULL (string));
    return FALSE;
  }

  gst_uri_normalize (uri);

  host = gst_uri_get_host (uri);
  if (!host) {
    g_set_error (error, GST_URI_ERROR, GST_URI_ERROR_BAD_REFERENCE,
        "URI lacks hostname: %s", string);
    goto out;
  }

  segments = gst_uri_get_path_segments (uri);
  nsegments = g_list_length (segments);
  if (nsegments != 3 || g_list_nth_data (segments, 0) != NULL) {
    g_set_error (error, GST_URI_ERROR, GST_URI_ERROR_BAD_REFERENCE,
        "URI path doesn't match /app/stream: %s", string);
    goto out;
  }

  port = gst_uri_get_port (uri);
  if (port == GST_URI_NO_PORT) {
    port = GST_RTMP_DEFAULT_PORT;
  }

  g_object_set (self, "host", host, "port", port,
      "application", g_list_nth_data (segments, 1),
      "stream", g_list_nth_data (segments, 2), NULL);

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
  g_list_free_full (segments, g_free);
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
