#! /bin/bash

# parse arguments
# -t tag, default is "pikadb/pika:<git tag>"
# -p platform , it will use docker buildx, options: all, linux/amd64, linux/arm64, linux/arm, linux/arm64, darwin/amd64  more details: https://docs.docker.com/build/building/multi-platform/
# --proxy proxy, proxy has no value,  if you want to use proxy, just add --proxy. if you are in China, you may need to use proxy download the package for up speed the build process
# --help help

while getopts "t:p:-:" opt; do
  case $opt in
    t)
      TAG=$OPTARG
      ;;
    p)
      PLATFORM=$OPTARG
      MULTIARCHIVE=true
      ;;
    -)
      case $OPTARG in
        proxy)
          proxy=1
          ;;
        help)
          echo "Usage: build_docker.sh [-t tag] [-p platform] [--proxy] [--help]"
          echo ""
          echo "Options:"
          echo "  -t tag,               default is \"pikadb/pika:<git tag>\""
          echo "  -p <plat>,[<plat>],   default is current docker platform. "
          echo "                        options: all, linux/amd64, linux/arm, linux/arm64"
          echo "                        more details: https://docs.docker.com/build/building/multi-platform "
          echo "  --proxy,              use proxy download the package for up speed the build process in CN."
          echo "  --help,               help"
          echo ""
          echo "eg:"
          echo "  ./build_docker.sh -p linux/amd64,linux/arm64 -t pikadb/pika:latest --proxy "
          exit 0
          ;;

        *)
          echo "Unknown option --$OPTARG"
          exit 1
          ;;
      esac
      ;;
    *)
      echo "Unknown option -$opt"
      exit 1
      ;;
  esac
done


# if TAG is not set, set it "pikadb/pika"
if [ -z "$TAG" ]
then
    TAG="pikadb/pika:$(git describe --tags --abbrev=0 --always)"
fi

# if Platform is not set, set it "linux/amd64"
if [ -z "$PLATFORM" ]
then
    PLATFORM="linux/amd64"
fi

# if Platform is "all", set it "linux/amd64,linux/arm64,linux/arm"
if [ "$PLATFORM" = "all" ]
then
    PLATFORM="linux/amd64,linux/arm,linux/arm64"
fi

# if proxy is set, set it
if [ -n "$proxy" ]
then
    PROXY=true
else 
    PROXY=false
fi

# check if docker is installed
if ! [ -x "$(command -v docker)" ]; then
  echo 'Error: docker is not installed.' >&2
  exit 1
fi


if [ "$MULTIARCHIVE" = true ]
then
    # check if `docker buildx inpsect pika-builder` is ok
    if ! docker buildx inspect pika-builder > /dev/null 2>&1; then
       docker buildx create --use --name=pika-builder --driver docker-container
    else
       docker buildx use pika-builder
    fi

    docker buildx build --platform ${PLATFORM} -t ${TAG} --build-arg ENABLE_PROXY=${PROXY} .

else
    # build single-arch image
    docker build -t ${TAG} --build-arg ENABLE_PROXY=${PROXY} .
fi


