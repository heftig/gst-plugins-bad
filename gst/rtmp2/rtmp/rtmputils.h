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
 * Free Software Foundation, Inc., 51 Franklin St, Fifth Floor,
 * Boston, MA 02110-1301, USA.
 */

#ifndef _GST_RTMP_UTILS_H_
#define _GST_RTMP_UTILS_H_

#include <glib.h>
#include "rtmpchunk.h"

G_BEGIN_DECLS

#define GST_RTMP_DEFAULT_PORT 1935

GBytes *gst_rtmp_bytes_remove (GBytes *bytes, gsize size);
gchar * gst_rtmp_hexify (const guint8 *src, gsize size);
guint8 * gst_rtmp_unhexify (const char *src, gsize *size);
gchar * gst_rtmp_tea_decode (const gchar *key, const gchar *text);

G_END_DECLS

#endif
