const { S3Client, PutObjectCommand } = require("@aws-sdk/client-s3");
const fs = require('fs');
const { createReadStream } = fs;

// Configure the AWS SDK for S3
const s3 = new S3Client({
  endpoint: 'http://127.0.0.1:4566',
  region: 'us-east-1',
  credentials: {
    accessKeyId: 'test',
    secretAccessKey: 'test',
  },
  forcePathStyle: true, // Needed for LocalStack
});

const bucketName = 'exam-review';
const fileName = 'example.txt';
const key = `new/${fileName}`

// Upload file to S3
async function uploadFile() {
  try {
    const fileStream = createReadStream(fileName);

    // Using the Upload class from AWS SDK v3 for managed uploads
    const putObjectParams = {
      Bucket: bucketName,
      Key: key,
      Body: fileStream,
    };

    // Upload the file
    await s3.send(new PutObjectCommand(putObjectParams));

    console.log(`File ${fileName} uploaded`);
  } catch (error) {
    console.error('Error uploading file:', error);
  }
}

async function run() {
  await uploadFile();
}

run();