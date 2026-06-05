export GOPROXY=https://goproxy.cn,direct
export GOSUMDB=sum.golang.google.cn

ARCH_NAME="${ARCH_NAME:-linux-amd64}"
IMAGE_PLATFORM="${IMAGE_PLATFORM:-linux/amd64}"
IMAGE_ARCH="${IMAGE_ARCH:-}"
if [ -z "$IMAGE_ARCH" ]; then
  case "${ARCH_NAME}" in
    linux-amd64)
      IMAGE_ARCH="amd64"
      ;;
    linux-arm64)
      IMAGE_ARCH="arm64"
      ;;
    *)
      echo "unsupported ARCH_NAME=${ARCH_NAME}; set IMAGE_ARCH explicitly" >&2
      exit 1
      ;;
  esac
fi
GOOS="${GOOS:-linux}"
if [ -z "${GOARCH:-}" ]; then
  case "${ARCH_NAME}" in
    linux-amd64)
      GOARCH="amd64"
      ;;
    linux-arm64)
      GOARCH="arm64"
      ;;
    *)
      echo "unsupported ARCH_NAME=${ARCH_NAME}; set GOARCH explicitly" >&2
      exit 1
      ;;
  esac
fi
CGO_ENABLED="${CGO_ENABLED:-0}"
ARCHIVE_DIR="${ARCHIVE_DIR:-/home/xl001/myroot/NewFramework/apps/${ARCH_NAME}}"
FTP_ROOT_URL="${FTP_ROOT_URL:-ftp://192.168.2.171/NewFramework}"
BASE_IMAGE_FTP_URL="${BASE_IMAGE_FTP_URL:-${FTP_ROOT_URL}/base-images/${ARCH_NAME}}"
APP_FTP_URL="${APP_FTP_URL:-${FTP_ROOT_URL}/apps/${ARCH_NAME}}"
FTP_USER="${FTP_USER:-data:data}"
USE_BUILDX="${USE_BUILDX:-false}"
BASE_IMAGE_NAME="alpine:3.23.4"
BASE_IMAGE_TAR="alpine-3.23.4-${ARCH_NAME}.tar"

echo $GOPROXY
echo "arch name: ${ARCH_NAME}"
echo "image platform: ${IMAGE_PLATFORM}"
echo "image arch: ${IMAGE_ARCH}"
echo "go target: ${GOOS}/${GOARCH}"
echo "use buildx: ${USE_BUILDX}"

load_base_image() {
  current_arch="$(docker image inspect --format '{{.Architecture}}' "$BASE_IMAGE_NAME" 2>/dev/null || true)"
  if [ "$current_arch" = "$IMAGE_ARCH" ]; then
    echo "base image exists locally: $BASE_IMAGE_NAME ($current_arch)"
    return
  fi

  echo "downloading base image from FTP: $BASE_IMAGE_TAR"
  rm -f "$BASE_IMAGE_TAR"
  curl -f -u "$FTP_USER" -O "${BASE_IMAGE_FTP_URL}/${BASE_IMAGE_TAR}"
  docker load -i "$BASE_IMAGE_TAR"
  rm -f "$BASE_IMAGE_TAR"
}

build_image() {
  if [ "$USE_BUILDX" = "true" ]; then
    docker buildx build --no-cache --pull=false --platform "$IMAGE_PLATFORM" --load -t "$CIMAGE_NAME_TAG" -f ./Dockerfile .
  else
    docker build --no-cache --pull=false --platform "$IMAGE_PLATFORM" -t "$CIMAGE_NAME_TAG" -f ./Dockerfile .
  fi
}

#进行git提交信息拉取，生成版本跟踪信息开始
rm -f version.txt
GIT_TIME=$(git log -1 | grep 'Date')
echo "git time:" $GIT_TIME >>version.txt
echo "git commit:" $GIT_COMMIT >>version.txt
echo "git no:" $BUILD_NUMBER >>version.txt
echo "git branch:" $GIT_BRANCH >>version.txt
#进行git提交信息拉取，生成版本跟踪信息结束

# 下载依赖前设置好代理
go mod download
go mod verify

rm -rf ./bin
mkdir -p ./bin

echo "编译二进制文件..."
export CGO_ENABLED
export GOOS
export GOARCH

# 构建
go build --tags netgo -trimpath -ldflags="-s -w" -o ./bin/server ./cmd/main.go

CIMAGE_NAME="new_mpc"
CIMAGE_DIST_TAG="basic_sys"
CIMAGE_NAME_TAG="$CIMAGE_NAME:$CIMAGE_DIST_TAG"
DOCKER_FILENAME="${DOCKER_FILENAME:-$CIMAGE_NAME-$CIMAGE_DIST_TAG-${ARCH_NAME}.tar}"
echo $DOCKER_FILENAME
rm -f $DOCKER_FILENAME

#先检查是否有容器在运行，有运行删除之
CONTAINER_ID=$(docker ps -a | grep -w "$CIMAGE_NAME" | awk '{print $1}')
if [ "$CONTAINER_ID" != "" ];then
  docker rm -f $CONTAINER_ID
fi

#删除本地库中的镜像开始
CIMAGE_ID=$(docker images | grep -w "$CIMAGE_NAME" | awk '{print $3}')
if [ "$CIMAGE_ID" != "" ];then
  docker rmi -f $CIMAGE_ID
fi
#删除本地库中的镜像结束


#打包新的镜像
load_base_image
build_image

#docker build -t $CIMAGE_NAME_TAG -f ./Dockerfile .
#导出docker tar
docker save $CIMAGE_NAME_TAG -o $DOCKER_FILENAME

#scp $DOCKER_FILENAME hadoop@192.168.1.80:/home/hadoop/dockerimages/qdsys
mkdir -p "$ARCHIVE_DIR"
rm -f "$ARCHIVE_DIR/$DOCKER_FILENAME"
cp $DOCKER_FILENAME "$ARCHIVE_DIR"
curl -u "$FTP_USER" -T $DOCKER_FILENAME "${APP_FTP_URL}/"
rm -f $DOCKER_FILENAME
