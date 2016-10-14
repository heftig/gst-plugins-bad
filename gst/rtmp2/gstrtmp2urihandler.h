/* GStreamer
 * Copyright (C) 2016 SMT Streaming Media Technologies GmbH
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

#ifndef __GST_RTMP2_URI_HANDLER_H__
#define __GST_RTMP2_URI_HANDLER_H__

#include <gst/gst.h>

G_BEGIN_DECLS
#define GST_TYPE_RTMP2_URI_HANDLER                 (gst_rtmp2_uri_handler_get_type ())
#define GST_RTMP2_URI_HANDLER(obj)                 (G_TYPE_CHECK_INSTANCE_CAST ((obj), GST_TYPE_RTMP2_URI_HANDLER, GstRtmp2URIHandler))
#define GST_IS_RTMP2_URI_HANDLER(obj)              (G_TYPE_CHECK_INSTANCE_TYPE ((obj), GST_TYPE_RTMP2_URI_HANDLER))
#define GST_RTMP2_URI_HANDLER_GET_INTERFACE(inst)  (G_TYPE_INSTANCE_GET_INTERFACE ((inst), GST_TYPE_RTMP2_URI_HANDLER, GstRtmp2URIHandlerInterface))
typedef struct _GstRtmp2URIHandler GstRtmp2URIHandler;  /* dummy object */
typedef struct _GstRtmp2URIHandlerInterface GstRtmp2URIHandlerInterface;

struct _GstRtmp2URIHandlerInterface
{
  GTypeInterface parent_iface;
};

GType gst_rtmp2_uri_handler_get_type (void);

void gst_rtmp2_uri_handler_implement_uri_handler (GstURIHandlerInterface *
    iface, GstURIType type);

gchar * gst_rtmp2_uri_handler_get_uri (GstRtmp2URIHandler * handler);
gboolean gst_rtmp2_uri_handler_set_uri (GstRtmp2URIHandler * handler,
    const gchar * uri);

typedef struct _GstRtmp2URI GstRtmp2URI;

struct _GstRtmp2URI
{
  gchar *host;
  guint port;
  gchar *application;
  gchar *stream;
};

void gst_rtmp2_uri_copy (GstRtmp2URI * dest, GstRtmp2URI * src);
void gst_rtmp2_uri_clear (GstRtmp2URI * uri);
gchar * gst_rtmp2_uri_get_string (GstRtmp2URI * uri, gboolean with_stream);

G_END_DECLS
#endif
