
plugindir = ${libdir}/gstreamer-1.0

plugin_LTLIBRARIES = libgstrtmp2.la

libgstrtmp2_la_SOURCES = gstrtmp2src.c gstrtmp2sink.c gstrtmp2.c

noinst_HEADERS = gstrtmp2src.h gstrtmp2sink.h
libgstrtmp2_la_CFLAGS = $(GST_PLUGINS_BASE_CFLAGS) $(GST_BASE_CFLAGS) $(GST_CFLAGS) $(GST_RTMP_CFLAGS)
libgstrtmp2_la_LIBADD = $(GST_PLUGINS_BASE_LIBS) $(GST_BASE_LIBS) $(GST_LIBS) $(GST_RTMP_LIBS) $(WINSOCK2_LIBS)
libgstrtmp2_la_LDFLAGS = $(GST_PLUGIN_LDFLAGS)
libgstrtmp2_la_LIBTOOLFLAGS = $(GST_PLUGIN_LIBTOOLFLAGS)
