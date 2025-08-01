const Records = require('./records.model');
const fs = require('fs');
const csv = require('csv-parser');

const upload = async (req, res) => {
    const { file } = req;
    console.log('📁 Archivo recibido:', file.originalname, 'Tamaño:', (file.size / (1024 * 1024)).toFixed(2), 'MB');

    if (!file) {
        console.log('❌ No se recibió ningún archivo');
        return res.status(400).json({ error: 'No se proporcionó ningún archivo' });
    }

    try {
        console.log('⏳ Iniciando procesamiento del archivo...');
        const batchSize = 500; // Reducir el tamaño del lote
        let batch = [];
        let processedCount = 0;
        let lineCount = 0;

        // Función para insertar un lote de registros
        const insertBatch = async (batch) => {
            if (batch.length === 0) return;
            
            try {
                await Records.insertMany(batch, { ordered: false });
                processedCount += batch.length;
                console.log(`✅ Procesadas ${lineCount} líneas - Insertados ${processedCount} registros`);
            } catch (err) {
                // Ignorar errores de duplicados (código 11000)
                if (err.code !== 11000) {
                    console.error('❌ Error insertando lote:', err);
                    throw err;
                }
            }
        };

        await new Promise((resolve, reject) => {
            const stream = fs.createReadStream(file.path)
                .pipe(csv())
                .on('data', async (data) => {
                    try {
                        lineCount++;
                        
                        // Progreso cada 10,000 líneas
                        if (lineCount % 10000 === 0) {
                            console.log(`📊 Procesando línea ${lineCount}...`);
                        }
                        
                        // Crear registro
                        batch.push({
                            id: parseInt(data.id, 10) || 0,
                            firstname: String(data.firstname || '').substring(0, 100),
                            lastname: String(data.lastname || '').substring(0, 100),
                            email: String(data.email || '').substring(0, 100),
                            email2: String(data.email2 || '').substring(0, 100),
                            profession: String(data.profession || '').substring(0, 100)
                        });

                        // Insertar lote cuando alcance el tamaño máximo
                        if (batch.length >= batchSize) {
                            stream.pause(); // Pausar el stream mientras se inserta
                            try {
                                await insertBatch([...batch]);
                                batch = [];
                                stream.resume();
                            } catch (err) {
                                stream.destroy();
                                reject(err);
                            }
                        }
                    } catch (error) {
                        console.error('❌ Error procesando línea:', error);
                        // Continuar con la siguiente línea
                    }
                })
                .on('end', async () => {
                    try {
                        console.log('🏁 Fin del archivo, insertando último lote...');
                        await insertBatch(batch);
                        console.log(`✨ Proceso completado. Total: ${lineCount} líneas procesadas, ${processedCount} registros insertados`);
                        resolve();
                    } catch (error) {
                        console.error('❌ Error finalizando el proceso:', error);
                        reject(error);
                    }
                })
                .on('error', (error) => {
                    console.error('❌ Error leyendo el archivo:', error);
                    reject(error);
                });
        });

        // Limpieza
        try {
            if (fs.existsSync(file.path)) {
                fs.unlinkSync(file.path);
                console.log('🧹 Archivo temporal eliminado');
            }
        } catch (error) {
            console.error('⚠️ Error eliminando archivo temporal:', error);
        }

        console.log(` Proceso completado. Total de registros procesados: ${processedCount}`);
        return res.status(200).json({ 
            message: 'Archivo procesado exitosamente',
            recordsProcessed: processedCount
        });

    } catch (error) {
        console.error(' Error en el proceso:', error);
        if (file && fs.existsSync(file.path)) {
            try {
                fs.unlinkSync(file.path);
                console.log(' Archivo temporal eliminado después de error');
            } catch (err) {
                console.error(' Error limpiando archivo temporal:', err);
            }
        }
        return res.status(500).json({ 
            error: 'Error al procesar el archivo',
            details: error.message 
        });
    }
};

const list = async (_, res) => {
    try {
        const data = await Records
            .find({})
            .limit(10)
            .lean();
        
        return res.status(200).json(data);
    } catch (err) {
        return res.status(500).json(err);
    }
};

module.exports = {
    upload,
    list,
};
