require('dotenv').config({path: '.env'});
const path = require('path');
const express = require('express');
const app = express();
const cors = require('cors');
const { Client } = require('minio');

const MINIO_CONFIG = {
  endpoint : process.env.MINIO_ENDPOINT || 'localhost',
  port : parseInt(process.env.MINIO_PORT || '9000'),
  usessl : process.env.MINIO_USESSL == 'true',
  accesskey : process.env.MINIO_ACCESSKEY || '',
  secretkey : process.env.MINIO_SECRETKEY || '',
}

app.use(cors());
app.set('view engine', 'pug');

let minioClient;
const port = parseInt(process.env.PORT || '5000');

function errorResponse(res, err) {
  res.status(500).json({
    result: 'error', message: err
  });
}


function serveListBuckets(req, res) {
  minioClient.listBuckets()
    .then(buckets => {
      let { endpoint, port } = MINIO_CONFIG;
      res.render('index', {buckets, endpoint, port});
    })
    .catch(err => {
      console.error(err);
      errorResponse(res, err);
    });
}

function extractBucketNameAndPrefix(subpath) {
  let elements = subpath.split('/');
  if (elements.length == 0) {
    throw new Error('no bucket name');
  }
  let bucketName = elements[0];
  let prefix = elements.length > 1 ? elements.slice(1).join('/') : '';
  return {
    bucketName, prefix
  }
}


function serveListObjects(req, res, subpath) {
  let { bucketName, prefix } = extractBucketNameAndPrefix(subpath);
  stream = minioClient.listObjectsV2(bucketName, prefix, false);

  let objects = [];

  stream.on('error', (err) => {
    console.error('MINIO listObjectsV2 stream error', err);
    errorResponse(res, 'error: no bucket name');
  });
  stream.on('data', (data) => {
    objects.push(data);
  });
  stream.on('end', () => {
    if (objects.length == 1 && objects[0].name == prefix) {
      serveObject(res, subpath);
    } else {
      let parent = prefix == '' ? '' : path.posix.dirname(prefix);
      res.render('dir', {
        parent, bucket: bucketName, prefix, list: objects
      });
    }
  });
}

function serveObject(res, subpath) {
  let { bucketName, prefix } = extractBucketNameAndPrefix(subpath);
  minioClient.getObject(bucketName, prefix, (err, stream) => {
    if (err) {
      errorResponse(res, err);
      return;
    }
    stream.pipe(res);
  });
}

app.get('/(*)', (req, res) => {
  let subpath = req.params[0];
  if (subpath == '') {
    serveListBuckets(req, res);
  } else {
    serveListObjects(req, res, subpath);
  }
});


app.listen(port, () => {
  console.log(`Server is listening on ${port}`);

  minioClient = new Client({
    endPoint: MINIO_CONFIG.endpoint,
    port: MINIO_CONFIG.port,
    useSSL: MINIO_CONFIG.usessl,
    accessKey: MINIO_CONFIG.accesskey,
    secretKey: MINIO_CONFIG.secretkey,
  });
});
