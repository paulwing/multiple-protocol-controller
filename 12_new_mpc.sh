source ./project.sh
CIMAGE_NAME="new_mpc"
CIMAGE_DIST_TAG="${PROJECT_NAME}_sys"
CONTAINER_NAME="new_mpc"
CIMAGE_NAME_TAG="$CIMAGE_NAME:$CIMAGE_DIST_TAG"
DOCKER_FILENAME="${CIMAGE_NAME}-${CIMAGE_DIST_TAG}.tar"

if [ ${USE_LOCALMEDIAL} != "true" ]; then
 rm -f ${DOCKER_FILENAME}
 curl -O ftp://${UPDATE_FTP_USERNAME}:${UPDATE_FTP_PWD}@${UPDATE_FTP_HOST_IP}/NewFramework/$DOCKER_FILENAME
fi

docker rm -f $CONTAINER_NAME
docker rmi $CIMAGE_NAME_TAG
docker load < $DOCKER_FILENAME
echo "start run " ${CONTAINER_NAME}
docker run -d --name $CONTAINER_NAME --restart=always \
	--network iot_net --network-alias ${CONTAINER_NAME} \
	-e "REDIS_HOST=iot-redis" \
	-e "REDIS_PORT=6379" \
	-e "REDIS_PWD=${IOT_REDIS_PWD}" \
	-e "MYSQL_HOST=iot-mysql" \
	-e "MYSQL_USER=${IOT_MYSQL_USER}" \
	-e "MYSQL_PORT=3306" \
	-e "IOT_MYSQL_PWD=${IOT_MYSQL_PWD}" \
	-e "IOT_MYSQL_DB=${IOT_MYSQL_DB}" \
	-e "DB_SALT=${IOT_DB_SALT}" \
	-e "DEC_PASSWORD=${IOT_DEC_PASSWORD}" \
	-e "XLSSO_ENABLE=${IOT_XLSSO_ENABLE}" \
	-e "IOT_APP_HOST=${IOT_APP_HOST}" \
	-v /etc/localtime:/etc/localtime:ro  \
	-v ~/dockervol/etc:/data/xlapps \
	-v ~/dockervol/static:/static/upload \
	-v ~/dockervol/upload:/upload \
	-p 19901:19901  \
	$CIMAGE_NAME_TAG
