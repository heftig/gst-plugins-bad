noinst_LTLIBRARIES = libgstrtmp2-rtmp.la

libgstrtmp2_rtmp_la_SOURCES = \
	amf.c \
	rtmpchunkstream.c \
	rtmpclient.c \
	rtmpconnection.c \
	rtmphandshake.c \
	rtmpinputstream.c \
	rtmpmessage.c \
	rtmpoutputstream.c \
	rtmputils.c \
	$(NULL)

libgstrtmp2_rtmp_la_CFLAGS = \
	$(GST_CFLAGS) \
	$(GIO_CFLAGS) \
	$(NULL)

libgstrtmp2_rtmp_la_LIBADD = \
	$(GST_LIBS) \
	$(GIO_LIBS) \
	$(NULL)

libgstrtmp2_rtmp_la_LDFLAGS = -module -avoid-version

# headers we need but don't want installed
noinst_HEADERS = \
	amf.h \
	rtmpchunkstream.h \
	rtmpclient.h \
	rtmpconnection.h \
	rtmphandshake.h \
	rtmpinputstream.h \
	rtmpmessage.h \
	rtmpoutputstream.h \
	rtmputils.h \
	$(NULL)

