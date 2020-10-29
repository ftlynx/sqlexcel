#!/usr/bin/env bash


CURR_PATH=$(cd `dirname $0`;pwd)
BNARY_NAME=`cat $CURR_PATH/go.mod  | grep ^module |awk '{print $2}'`

function _info(){
	local msg=$1
	local now=`date '+%Y-%m-%d %H:%M:%S'`
	echo  "\033[1;46;30m[INFO]\033[0m $now $msg"
}

function _version(){
	local msg=$1
	local now=`date '+%Y-%m-%d %H:%M:%S'`
	echo  "\033[1;46;30m[INFO]\033[0m $now $msg"
}

function get_tag () {
	local tag=$(git describe --tags 2>>/dev/null)
	if [ -n "$tag" ];then
		tag=$(echo $tag | cut -d '-' -f 1)
	else
		tag='unknown'
	fi
	echo $tag
}

function get_branch () {
	local branch=$(git rev-parse --abbrev-ref HEAD)
	if [ -z "$branch" ]; then
    branch='unknown'
	fi
	echo $branch
}

function get_commit () {
	local commit=$(git rev-parse HEAD)
	if [ -z "$commit" ]; then
		commit='unknown'
	fi
	echo $commit
}


function main() {
	local platform=$1
	local bin_name=$2
	local main_file=$3
	local is_docker=$4

	_info "开始构建 [$BNARY_NAME] ..."

	TAG=$(get_tag)
	BRANCH=$(get_branch)
	COMMIT=$(get_commit)
	DATE=$(date '+%Y-%m-%d %H:%M:%S')
	version=$(go version | grep -o  'go[0-9].[0-9].*')

	Path="$BNARY_NAME/version"
	_version "构建版本的时间(Build Time): $DATE"
	_version "当前构建的版本(Git   Tag ): $TAG"
	_version "当前构建的分支(Git Branch): $BRANCH"
	_version "当前构建的提交(Git Commit): $COMMIT"

	case $platform in
	"linux")
		_info "开始构建Linux平台版本 ..."
		GOOS=linux GOARCH=amd64 \
		CGO_ENABLED=0 go build -a -o $bin_name -ldflags "-X '$Path.GIT_TAG=$TAG' -X '$Path.GIT_BRANCH=$BRANCH' -X '$Path.GIT_COMMIT=$COMMIT' -X '$Path.BUILD_TIME=$DATE' -X '$Path.GO_VERSION=$version'" $main_file
		;;
	*)
		_info "开始本地构建 ..."
		CGO_ENABLED=0 go build -a -o $bin_name -ldflags "-X '$Path.GIT_TAG=$TAG' -X '$Path.GIT_BRANCH=$BRANCH' -X '$Path.GIT_COMMIT=$COMMIT' -X '$Path.BUILD_TIME=$DATE' -X '$Path.GO_VERSION=$version'" $main_file
		;;
	esac
	if [ $? -ne 0 ];then
	    _info "构建失败"
		exit 1
	fi
	_info "程序构建完成: $bin_name"
}

main ${1:-local} $BNARY_NAME $CURR_PATH/main.go

