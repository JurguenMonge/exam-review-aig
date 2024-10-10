const { S3Client, GetObjectCommand, PutObjectCommand } = require('@aws-sdk/client-s3');

// Configurar S3 Client apuntando a LocalStack
const s3 = new S3Client({
  // Especifica el endpoint de LocalStack. Se usa una dirección IP interna de Docker.
  //endpoint: 'http://host.docker.internal:4566',
  endpoint: "http://172.17.0.2:4566",
  region: 'us-east-1', // Define la región de AWS, aquí se utiliza 'us-east-1'.
  credentials: {
    accessKeyId: 'test', // Clave de acceso ficticia para LocalStack.
    secretAccessKey: 'test', // Secreto ficticio para LocalStack.
  },
  forcePathStyle: true, // Es necesario forzar el uso de estilo de ruta en LocalStack.
});

// Función Lambda que se ejecuta cuando se produce el evento.
exports.handler = async (event) => {
  try {
    // Obtener el nombre del bucket y la clave del objeto del evento S3.
    const bucket = event.Records[0].s3.bucket.name;
    const key = event.Records[0].s3.object.key;
    const folderDest = "processed/"; // Carpeta de destino donde se almacenarán las líneas procesadas.
    console.log("Key:", key); // Imprime la clave del archivo para fines de depuración.

    // Parámetros para obtener el archivo de S3.
    const getObjectParams = {
      Bucket: bucket,
      Key: key
    };

    // Recupera el archivo de S3.
    const fileData = await s3.send(new GetObjectCommand(getObjectParams));

    // Convierte el cuerpo del archivo a una cadena UTF-8 (string).
    const body = Buffer.from(await fileData.Body.transformToByteArray()).toString('utf8');

    // Divide el contenido del archivo en líneas.
    const lines = body.split("\n");

    // Itera sobre cada línea y la guarda como un archivo separado en S3.
    for (let i = 0; i < lines.length; i++) {
      const lineKey = `${folderDest}${key}-line-${i + 1}.txt`; // Clave del nuevo archivo basado en el índice de la línea.
      const putObjectParams = {
        Bucket: bucket,
        Key: lineKey, // Clave del archivo de salida.
        Body: lines[i], // Contenido de la línea.
      };
      await s3.send(new PutObjectCommand(putObjectParams)); // Envía el comando para guardar la línea en S3.
    }

    // Si todo se procesó correctamente, imprime un mensaje de éxito y retorna un status 200.
    console.log("File processed successfully");
    return {
      statusCode: 200,
      body: JSON.stringify({ message: "File processed successfully" }),
    };
  } catch (error) {
    // Si ocurre un error, se captura, se imprime en la consola y se devuelve un status 500.
    console.error(error);
    return {
      statusCode: 500,
      body: JSON.stringify({ message: "Error processing file", error: error.message }),
    };
  }
}
