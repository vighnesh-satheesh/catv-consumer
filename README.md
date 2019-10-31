# Portal Rest API Server



## Requirements
 * python 3.6+
 * virtualenv
 * libmagic 5.33+


## How To Run(For the development)

1. Go to root directory and Run virtualenv

	```
	$ virtualenv env
	$ source env/bin/activate
	```
	
2. Initialize and update submodule.

	```
	$ git submodule init
	$ git submodule update
	```
	
	NOTE: Alternatively, you can initialize and update submodule when cloning.
	
	For git > v2.13,
	
	```
	$ git clone --recurse-submodules -j2 https://bitbucket.org/uppsalafoundation/portal-api.git
	```

2. Install dependent packages

	* Ubuntu
	
	```
    $ sudo apt install libmagic
	$ pip install -r requirements/development.txt
	$ cd library/indicator-lib/src/py && python setup.py install
	```
	
	* Mac
	
	```
    $ brew install libmagic
	$ pip install -r requirements/development.txt
	$ cd library/indicator-lib/src/py && python setup.py install
	```
	
3. Set two enviroment variables. **PORTAL\_API\_ENV** and **PORTAL\_API\_ENV\_PATH**.

	* **PORTAL\_API\_ENV**
		* one of `development` or `production`
	* **PORTAL\_API\_ENV\_PATH**
		* environment file path.

	```
	$ export PORTAL_API_ENV=development
	$ export PORTAL_API_ENV_PATH=/env/file/path.env
	```
	
4. Create *.env* file at the path stored at **PORTAL\_API\_ENV\_PATH**. Please check *sample.env.example* and refer to Environment Variables.
	
	```
	$ echo "DATABASE_URL=psql://...." > development.env
    $ echo "CACHE_URL=rediscache://host:port" >> development.env
    $ echo "REDIS_TOKEN_URL=rediscache://host:port/dbnum?timeout=3600" >> development.env
    $ echo "API_MEDIA_ROOT=fullpath" >> development.env
    ...
	```

5. Create aws *config* and *credentials* file under *~/.aws/*.
	* NOTE: This is only for local development. In AWS environment, AWS Role should be assigned.

	* *~/.aws/credentials*
	
	```
	[default]
	aws_access_key_id = ...
	aws	_secret_access_key = ...
	```
	
	* *~/.aws/config*
	
	```
	[default]
	region=ap-northeast-2
	```
	
	
6. Run django server.

	```
	$ python manage.py runserver
	```
	
## Environment Varaibles(*.env*)

* DATABASE\_URL
	* **Required**
	* database url.
	* default. *None*
* CACHE\_URL
 	* **Required**
	* redis cache url.
	* default. *None*
* REDIS\_TOKEN\_URL
  	* **Required**
	* authentication token redis url.
	* default. *None*
* CASE\_LIST\_DETAIL\_LEN
	* default. *300*
* CASE\_TITLE\_MAX\_LEN
	* Maximum Case title length.
	* Default *128 characters*
* CASE\_DETAIL\_MAX\_LEN
	* Maximum Case detail length.
	* Default *4096 characters*
* CASE\_REPORTER\_MAX\_LEN
	* Maximum Case reporter length.
	* Default *128 characters*
* CASE\_SECURITY\_TAGS\_LIMIT
* CASE\_ATTACHED\_FILE\_MAX\_LIMIT
	* Maximum number of attached file per Case
	* Default *20*
* INDICATOR\_LIST\_DETAIL\_LEN
	* default. *300*
* INDICATOR\_DETAIL\_MAX\_LEN
	* Maximum Indicator detail length.
	* Default *4096 characters*
* INDICATOR\_PATTERN\_MAX\_LEN
	* Maximum Indicator pattern length
	* Default *256 characters*
* ICO\_LIST\_DETAIL\_LEN
	* default. *300* 
* API\_TRDB\_API\_URL
 	* **Required**
	* TRDB Api url.
	* default. *http://localhost:3001/v1/*
* API\_ATTACHED\_FILE\_S3\_REGION
 	* **Required**
    * Attached file bucket region
    * default. `AWS_REGION` or `ap-northeast-2`
* API\_ATTACHED\_FILE\_S3\_BUCKET\_NAME
 	* **Required**
    * Attached file bucket name
    * default. *None*
* API\_ATTACHED\_FILE\_S3\_KEY\_PREFIX
 	* **Required**
    * Attached file key prefix.
    * default. *files/*'
* API\_ATTACHED\_FILE\_MEDIA\_URL
 	* **Required**
    * attached file media url
* API\_ATTACHED\_FILE\_MIN\_SIZE
	* Attached file minimum size.
	* default *50 bytes*
* API\_ATTACHED\_FILE\_NAME\_MAX\_LEN
	* Attached file name length limit.
	* default *256 characters*
* API\_ATTACHED\_FILE\_UPLOAD\_NUM\_LIMIT
	* Attached file the number of upload per request limit.
	* default *1*
* API\_ICO\_IMAGE\_S3\_REGION
 	* **Required**
    * Attached file bucket region
    * default. `AWS_REGION` or `ap-northeast-2`
* API\_ICO\_IMAGE\_S3\_BUCKET\_NAME
 	* **Required**
    * ICO image bucket name
    * default. *None*
* API\_ICO\_IMAGE\_S3\_KEY\_PREFIX
 	* **Required**
    * ICO image key prefix.
    * default. *image/*'
* API\_ICO\_IMAGE\_MEDIA\_URL
 	* **Required**
    * ICO image media url
* API\_TOKEN\_ENCRYPT\_PRIVATE\_KEY
	* **Required**
	* token and password encryption private key file path.
* API\_SENTRY\_DSN
	* **Required** for `production`
	* Sentry DSN. This is for `production`
* API_S3_REGION
    * **Required**
    * specifies the S3 region
* API_S3_BUCKET_NAME 
    * **Required**
    * specifies the s3 bucket name where image files are uploaded.
* API_S3_IMAGE_MEDIA_URL
    * **Required**
    * settings.py's MEDIA_URL
* API_S3_ICO_IMAGE_KEY_PREFIX
    * **Required**
    * s3 folder name (ico images)
* API_S3_ICO_IMAGE_DEFAULT
    * **Required**
    * ico default image (fallback ico image)
* API_S3_ICO_IMAGE_KEY_PREFIX
    * **Required**
    * s3 folder name (user images)
* API_S3_ICO_IMAGE_DEFAULT
    * **Required**
    * user default image (fallback user image)
* API_CELERY_BROKER_URL
    * **Required**
    * celery broker url (redis)
* API_CELERY_RESULT_BACKEND
    * **Required**
    * celery result backend (redis)
* API_EMAIL_HOST_USER
    * **Required**
    * AWS user access key for sending emails
* API_EMAIL_HOST_PASSWORD
    * **Required**
    * AWS user secret key for sending emails
* API_WEB_URL
    * **Required**
    * portal's web url address
* STATIC\_ROOT
	* static file root for admin. 

## How To Build Docker ImageAPI_S3_BUCKET_NAMEAPI

### portal-file-api

1. Run docker build command on the root directory.

	```
	$ docker build . --build-arg SLACK_URL={slack webhook url} --build-arg PM2_CONFIG_FILE=pm2_file.json --build-arg EXPOSE_FILE_API=true --build-arg PORTAL_API_VERSION={api server version} -t portal-file-api:{tag name}
	```
	* SLACK\_URL
		* slack notification when process restart.
	* PM2\_CONFIG\_FILE
		* pm2 config file name. 
	* EXPOSE\_FILE\_API
		* expose file api. 
	* PORTAL\_API\_VERSION
		* portal api server version, not api version.
		* this is for Sentry release tag. 

### portal-api

1. Run docker build command on the root directory.

	```
	$ docker build . --build-arg SLACK_URL={slack webhook url} --build-arg PM2_CONFIG_FILE=pm2.json --build-arg EXPOSE_GENERAL_API=true --build-arg PORTAL_API_VERSION={api server version} -t portal-api:{tag name}
	```
	* SLACK\_URL
		* slack notification when process restart.
	* PM2\_CONFIG\_FILE
		* pm2 config file name. 
	* EXPOSE\_GENERAL\_API
		* expose general api except file api. 
	* PORTAL\_API\_VERSION
		* portal api server version, not api version.
		* this is for Sentry release tag. 

### portal-admin

1. Collect static using django.
	
	Default static root is *./static*
	
	```
	$ python manage.py collectstatic
	```
	
2. Run docker build command on the root directory.

	```
	$ docker build . -f Dockerfile_admin --build-arg PM2_CONFIG_FILE=pm2_admin.json --build-arg SLACK_URL={slack webhook url} --build-arg ALLOWED_HOSTS=localhost -t portal-admin
	```
	* SLACK\_URL
		* slack notification when process restart.
	* PM2\_CONFIG\_FILE
		* pm2 config file name. 


## HOW To Push Docker Image to AWS.

1. Retrieve AWS login command

	```
	$ aws ecr get-login --no-include-email --region ap-northeast-2
	```
	This command will print out login command like below.
	
	```
	docker login -u AWS -p {LONG PASSWORD STRING} {CONTAINER REGISTRY URL}
	```
	After copying all above command from the terminal, paste and run.
	
2. Add tag.

	```
	$ docker tag {portal-api|portal-file-api}:{tag name} {registry uri}:{tag name}
	```
	
3. Push to AWS Registry.

	```
	$ docker push {registry uri}:{tag name}
	```
	
	Done.


## How To Run Docker Image.

### Using AWS Parameter Store.

CAUTION: Instance should have read permission to AWS SysmemManager ParameterStore.
	
* You have to pass `PORTAL\_API_PARAM_PATH` env variable.
	* same parameter for `portal-file-api` and `portal-api`  
	* ex.) PORTAL\_API\_PARAM\_PATH = /DeploymentConfig/PRD/portal-api
* You can pass AWS credential using env variables.
	* AWS\_ACCESS\_KEY\_ID
	* AWS\_SECRET\_ACCESS\_KEY
	* AWS\_REGION
* Expose portal-api port(8000). (pm2 healthcheck port(3333) is optional).
* Run without `CMD` or `ENTRYPOINT`. Image already has default `CMD`.
* Memory Usage: 100MB ~ 200MB.
	
### Using Container ENV Variables. (Not recommended)

* You can pass all the configurations using environment variables. Please check `Environment Variables` section.
	

## Scripts

### insert\_trdb\_data

Inserting Cases which are in RELEASED status to `trdb_case_transaction` table using trdb api server.

NOTE: For the development, it inserts all the case regardless of case status.

```
$ python manage.py runscript scripts.insert_trdb_data {production|development}
```

## Setting up development environment with Docker
1. The `docker-compose.yml` file defines a webapi service which does the following:
    1. Build a docker image with the `Dockerfile_dev` file
    2. Map the host code directory to the `app` directory inside the to-be built container.
    3. Define a few environment variables, which are used during the project initialization phase by Django.
    Most importantly, `PORTAL_API_ENV_PATH` & `API_TOKEN_ENCRYPT_PRIVATE_KEY`. You need to grab a copy of them from someone 
    if you don't have it. If you already have the files, then inside the file for the `PORTAL_API_ENV_PATH` change the DATABASE 
    variables to use `host.docker.internal` so that the Postgres database on your host machine can be used by the Docker container.
    4. Port forwarding from host 8000 to container 8000.

2. Assuming you have the two files with you, paste them inside the project root directory.
3. Run `docker-compose up` to start the api server.

Note:
The uwsgi reload time is specified as 5 seconds in the uwsgi_dev.ini file. So anytime you change some code inside this project the uwsgi server will be reloaded 
inside the container within 5 seconds. So you do not need to rebuild the image. Feel free to tune the `py-autoreload` variable according to your development needs. 