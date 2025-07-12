const core = require('@actions/core');
const S3 = require('aws-sdk/clients/s3');
const fs = require('fs');
const path = require('path');
const shortid = require('shortid');
const slash = require('slash').default;
const klawSync = require('klaw-sync');
const { lookup } = require('mime-types');
const crypto = require('crypto');

const AWS_KEY_ID = core.getInput('aws_key_id', {
  required: true,
});
const SECRET_ACCESS_KEY = core.getInput('aws_secret_access_key', {
  required: true,
});
const BUCKET = core.getInput('aws_bucket', {
  required: true,
});
const SOURCE_DIR = core.getInput('source_dir', {
  required: true,
});
const DESTINATION_DIR = core.getInput('destination_dir', {
  required: false,
});
const ENDPOINT = core.getInput('endpoint', {
  required: false,
});

const s3options = {
  accessKeyId: AWS_KEY_ID,
  secretAccessKey: SECRET_ACCESS_KEY,
};

if (ENDPOINT) {
  s3options.endpoint = ENDPOINT;
}

const s3 = new S3(s3options);
const destinationDir = DESTINATION_DIR === '/' ? shortid() : DESTINATION_DIR;
const paths = klawSync(SOURCE_DIR, {
  nodir: true,
});

function calculateFileSHA1(filePath) {
  return new Promise((resolve, reject) => {
    const hash = crypto.createHash('sha1');
    const stream = fs.createReadStream(filePath);

    stream.on('error', (err) => reject(err));
    stream.on('data', (chunk) => hash.update(chunk));
    stream.on('end', () => resolve(hash.digest('hex')));
  });
}

function getRemoteSHA1(bucket, key) {
  return new Promise((resolve, reject) => {
    s3.headObject({ Bucket: bucket, Key: key }, (err, data) => {
      if (err) {
        if (err.code === 'NotFound') return resolve(null);
        return reject(err);
      }
      const sha1 = data.Metadata?.sha1 || null;
      resolve(sha1);
    });
  });
}

async function uploadWithSha1Check(localPath, bucketPath) {
  const sha1 = await calculateFileSHA1(localPath);
  const remoteSha1 = await getRemoteSHA1(BUCKET, bucketPath);

  if (remoteSha1 && remoteSha1 === sha1) {
    core.info(`skip upload (sha1 match) - ${bucketPath}`);
    return `skipped://${bucketPath}`;
  }

  const fileStream = fs.createReadStream(localPath);
  const params = {
    Bucket: BUCKET,
    ACL: 'public-read',
    Body: fileStream,
    Key: bucketPath,
    ContentType: lookup(localPath) || 'application/octet-stream',
    Metadata: {
      sha1: sha1,
    },
  };

  return upload(params);
}

function run() {
  return Promise.all(
    paths.map((p) => {
      const filename = slash(p.path).split('/').pop();
      const bucketPath = slash(path.join(destinationDir, filename));
      return uploadWithSha1Check(p.path, bucketPath);
    })
  );
}

run()
  .then((locations) => {
    core.info(`object key - ${destinationDir}`);
    core.info(`object locations - ${locations}`);
    core.setOutput('object_key', destinationDir);
    core.setOutput('object_locations', locations);
  })
  .catch((err) => {
    core.error(err);
    core.setFailed(err.message);
  });
