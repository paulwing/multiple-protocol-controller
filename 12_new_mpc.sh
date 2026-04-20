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
if [[ ! ${ONLY_LOAD_IMAGES} ]] || [[ ${ONLY_LOAD_IMAGES} != "true" ]]; then
	echo "start run " ${CONTAINER_NAME}
	if [ ${USE_PRIVATE_NET} == "true" ]; then
		docker run -d --name $CONTAINER_NAME --restart=always \
		--network iot_net --network-alias ${CONTAINER_NAME} \
		-e "REDIS_HOST=redis6" -e "REDIS_PORT=${IOT_REDIS_PORT}" \
		-e "MYSQL_HOST=mysql_5732" \
		-e "MYSQL_USER=${IOT_MYSQL_USER}" \
		-e "MYSQL_PORT=${IOT_MYSQL_PORT}" \
		-e "IOT_MYSQL_PWD=${IOT_MYSQL_PWD}" \
		-e "IOT_MYSQL_DB=${IOT_MYSQL_DB}" \
		-e "DB_SALT=${IOT_DB_SALT}" \
		-e "DEC_PASSWORD=${IOT_DEC_PASSWORD}" \
		-e "XLSSO_ENABLE=${IOT_XLSSO_ENABLE}" \
		-e "IOT_APP_HOST=${IOT_APP_HOST}" \
		-v /etc/localtime:/etc/localtime:ro  \
		-v ~/dockervol/etc:/data/xlapps \
		-v ~/dockervol/static:/static/upload -v \
		~/dockervol/upload:/upload -p ${IOT_CONFIG_PORT}:8088  \
		$CIMAGE_NAME_TAG
	else 
		docker run -d --name $CONTAINER_NAME --restart=always \
		-e "REDIS_HOST=${IOT_REDIS_HOST}" \
        -e "REDIS_PORT=${IOT_REDIS_PORT}" \
		-e "REDIS_PWD=${IOT_REDIS_PWD}" \
		-v /etc/localtime:/etc/localtime:ro  \
		-v ~/dockervol/static:/static/upload -v \
		~/dockervol/upload:/upload -p 19901:19901  \
		$CIMAGE_NAME_TAG
	fi
fi