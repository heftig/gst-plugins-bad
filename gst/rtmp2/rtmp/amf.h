/* GStreamer RTMP Library
 * Copyright (C) 2014 David Schleef <ds@schleef.org>
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

#ifndef _GST_RTMP_AMF_H_
#define _GST_RTMP_AMF_H_

#include <glib.h>

G_BEGIN_DECLS

typedef enum {
  GST_AMF_TYPE_INVALID = -1,
  GST_AMF_TYPE_NUMBER = 0,
  GST_AMF_TYPE_BOOLEAN = 1,
  GST_AMF_TYPE_STRING = 2,
  GST_AMF_TYPE_OBJECT = 3,
  GST_AMF_TYPE_MOVIECLIP = 4,
  GST_AMF_TYPE_NULL = 5,
  GST_AMF_TYPE_UNDEFINED = 6,
  GST_AMF_TYPE_REFERENCE = 7,
  GST_AMF_TYPE_ECMA_ARRAY = 8,
  GST_AMF_TYPE_OBJECT_END = 9,
  GST_AMF_TYPE_STRICT_ARRAY = 10,
  GST_AMF_TYPE_DATE = 11,
  GST_AMF_TYPE_LONG_STRING = 12,
  GST_AMF_TYPE_UNSUPPORTED = 13,
  GST_AMF_TYPE_RECORDSET = 14,
  GST_AMF_TYPE_XML_DOCUMENT = 15,
  GST_AMF_TYPE_TYPED_OBJECT = 16,
  GST_AMF_TYPE_AVMPLUS_OBJECT = 17
} GstAmfType;


struct _GstAmfNode {
  GstAmfType type;
  guint32 int_val;
  gdouble double_val;
  gchar *string_val;
  GPtrArray *array_val;
};
typedef struct _GstAmfNode GstAmfNode;

GstAmfNode * gst_amf_node_new (GstAmfType type);
void gst_amf_node_free (GstAmfNode *node);
void gst_amf_node_dump (GstAmfNode *node);

GstAmfNode * gst_amf_node_new_parse (const guint8 *data, gsize size,
    gsize *n_bytes);

void gst_amf_node_set_boolean (GstAmfNode *node, gboolean val);
void gst_amf_node_set_number (GstAmfNode *node, gdouble val);
void gst_amf_node_set_string (GstAmfNode *node, const gchar *s);
void gst_amf_node_set_binary_string_take (GstAmfNode *node, guint8 *s, gsize size);
void gst_amf_node_set_string_take (GstAmfNode *node, gchar *s);
void gst_amf_node_set_ecma_array (GstAmfNode *node, guint8 *data, guint size);
void gst_amf_object_append_take (GstAmfNode *node, const gchar *s,
    GstAmfNode *child_node);

gboolean gst_amf_node_get_boolean (const GstAmfNode *node);
const gchar *gst_amf_node_get_string (const GstAmfNode *node);
double gst_amf_node_get_number (const GstAmfNode *node);
const GstAmfNode *gst_amf_node_get_object (const GstAmfNode *node, const gchar *field_name);
guint gst_amf_node_get_object_length (const GstAmfNode *node);
const GstAmfNode *gst_amf_node_get_object_by_index (const GstAmfNode *node, guint i);

void gst_amf_object_set_number (GstAmfNode *node, const char *field_name,
    gdouble val);
void gst_amf_object_set_string_take (GstAmfNode *node, const char *field_name,
    gchar *s);
void gst_amf_object_set_string (GstAmfNode *node, const char *field_name,
    const gchar *s);

GBytes * gst_amf_serialize_command (const char *command_name,
    gdouble transaction_id, GstAmfNode *command_object, GstAmfNode *optional_args);
GBytes * gst_amf_serialize_command2 (const char *command_name,
    gdouble transaction_id, GstAmfNode *command_object, GstAmfNode *optional_args,
    GstAmfNode *n3, GstAmfNode *n4);

G_END_DECLS

#endif
