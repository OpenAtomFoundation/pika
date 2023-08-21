# nodejs/llhttp
FETCHCONTENT_DECLARE(
        llhttp
        URL https://github.com/nodejs/llhttp/archive/refs/tags/release/v6.0.5.tar.gz
        URL_HASH MD5=7ec6829c56642cce27e3d8e06504ddca
        DOWNLOAD_NO_PROGRESS 1
        UPDATE_COMMAND ""
        LOG_CONFIGURE 1
        LOG_BUILD 1
        LOG_INSTALL 1
        BUILD_ALWAYS 1
        CMAKE_ARGS
        -DCMAKE_INSTALL_PREFIX=${STAGED_INSTALL_PREFIX}
        -DCMAKE_BUILD_TYPE=${LIB_BUILD_TYPE}
        -DBUILD_STATIC_LIBS=ON
        -DBUILD_SHARED_LIBS=OFF
        BUILD_COMMAND make -j${CPU_CORE}
)
FETCHCONTENT_MAKEAVAILABLE(llhttp)