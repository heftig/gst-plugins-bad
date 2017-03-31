/* GStreamer RTMP Library
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

#ifndef _GST_RTMP_OUTPUT_STREAM_H_
#define _GST_RTMP_OUTPUT_STREAM_H_

#include <gio/gio.h>
#include <gst/gst.h>

G_BEGIN_DECLS

#define GST_TYPE_RTMP_OUTPUT_STREAM    (gst_rtmp_output_stream_get_type())
#define GST_RTMP_OUTPUT_STREAM(obj)    (G_TYPE_CHECK_INSTANCE_CAST((obj),GST_TYPE_RTMP_OUTPUT_STREAM,GstRtmpOutputStream))
#define GST_IS_RTMP_OUTPUT_STREAM(obj) (G_TYPE_CHECK_INSTANCE_TYPE((obj),GST_TYPE_RTMP_OUTPUT_STREAM))

typedef struct _GstRtmpOutputStream GstRtmpOutputStream;

GType gst_rtmp_output_stream_get_type (void);

GstRtmpOutputStream *gst_rtmp_output_stream_new (GOutputStream * stream);

void gst_rtmp_output_stream_write_async (GstRtmpOutputStream * stream,
    guint32 chunk_stream, GstBuffer * buffer, GCancellable * cancellable,
    GAsyncReadyCallback callback, gpointer user_data);

gboolean gst_rtmp_output_stream_write_finish (GstRtmpOutputStream * stream,
    GAsyncResult * result, GError ** error);

G_END_DECLS
#endif
