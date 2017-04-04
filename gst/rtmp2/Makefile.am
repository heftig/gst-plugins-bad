plugin_LTLIBRARIES = libgstrtmp2.la

SUBDIRS = rtmp

libgstrtmp2_la_SOURCES = \
	gstrtmp2.c \
	gstrtmp2locationhandler.c \
	gstrtmp2sink.c \
	gstrtmp2src.c \
	$(NULL)

libgstrtmp2_la_CFLAGS = \
	$(GST_PLUGINS_BASE_CFLAGS) \
	$(GST_BASE_CFLAGS) \
	$(GST_CFLAGS) \
	$(GIO_CFLAGS) \
	$(NULL)

libgstrtmp2_la_LIBADD = \
	$(top_builddir)/gst/rtmp2/rtmp/libgstrtmp2-rtmp.la \
	$(GST_PLUGINS_BASE_LIBS) \
	$(GST_BASE_LIBS) \
	$(GST_LIBS) \
	$(GIO_LIBS) \
	$(NULL)

libgstrtmp2_la_LDFLAGS = $(GST_PLUGIN_LDFLAGS)
libgstrtmp2_la_LIBTOOLFLAGS = $(GST_PLUGIN_LIBTOOLFLAGS)

# headers we need but don't want installed
noinst_HEADERS = \
	gstrtmp2locationhandler.h \
	gstrtmp2sink.h \
	gstrtmp2src.h \
	$(NULL)
