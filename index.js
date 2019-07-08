const { planaria, planarium } = require("neonplanaria");
const express = require('express');
const fs = require('fs-extra');
const crypto = require('crypto');
const lmdb = require('node-lmdb');
let fspath = './data';
let fullpath = `${__dirname}/data/`;

const B = '19HxigV4QyBv3tHpQVcUEQyq1pzZVdoAut';
const BCAT = '15DHFxWZJT58f9nhyGnsRBqrgwK4W6h4Up';
const BCHUNK = '1ChDHzdd1H4wSjgGMHyndZm6qxEDGjqpJL';

fs.ensureDirSync('./lmdb');

let en = new lmdb.Env();
en.open({
    path: "./lmdb",
    mapSize: 2 * 1024 * 1024 * 1024,
    maxDbs: 3
});
const db = en.openDbi({ name: "mimetype", create: true })

async function saveMetadata(id, contentType) {
    if(!contentType) return;
    console.log(`Saving Metadata ${id} - ${contentType}`);
    let txn = en.beginTxn();
    txn.putString(db, id, contentType);
    txn.commit();
}

async function streamFile(destPath, sourcePath, options) {
    return new Promise((resolve, reject) => {
        const write = fs.createWriteStream(destPath, options)
            .on('error', reject)
            .on('close', resolve);
        fs.createReadStream(sourcePath)
            .on('error', reject)
            .pipe(write);
    })
}

async function hashFile(filename) {
    const sum = crypto.createHash('sha256');
    return new Promise((resolve, reject) => {
        const fileStream = fs.createReadStream(filename);
        fileStream.on('error', reject);
        fileStream.on('data', (chunk) => {
            try {
                sum.update(chunk)
            } catch (err) {
                reject(err)
            }
        });
        fileStream.on('end', () => {
            resolve(sum.digest('hex'))
        })
    });
}

async function createCLink(source) {
    let hash = await hashFile(source);
    const filePath = `${fspath}/c/${hash}`;
    if (!await fs.pathExists(filePath)) {
        await fs.rename(source, filePath);
    }
    else {
        await fs.unlink(source);
    };
    await fs.link(filePath, source);

    return hash;
}

async function saveB(txId, opRet) {
    if (await fs.pathExists(`${fspath}/b/${txId}`)) return;

    const data = opRet.lb2 || opRet.b2 || '';
    if (typeof data !== 'string') return;
    buffer = Buffer.from(data, 'base64');

    const fileData = {
        info: 'b',
        contentType: opRet.s3,
        encoding: opRet.s4,
        filename: opRet.s5
    };

    console.log("Saving B: ", txId)
    const bPath = `${fspath}/b/${txId}`;
    await fs.writeFile(bPath, buffer);
    const hash = await createCLink(bPath);
    await saveMetadata(`b/${txId}`, fileData.contentType);
    await saveMetadata(`c/${hash}`, fileData.contentType);
}

async function saveChunk(txId, opRet) {
    const filepath = `${fspath}/chunks/${txId}`;
    if (await fs.pathExists(filepath)) return;

    console.log(`Saving Chunk: ${txId}`);
    const data = opRet.lb2 || opRet.b2 || '';
    if (typeof data !== 'string') return;
    buffer = Buffer.from(data, 'base64');

    await fs.writeFile(filepath, buffer);
}

async function saveBCat(bcat) {
    const destPath = `${fspath}/bcat/${bcat.txId}`;
    if (!bcat.chunks.length || await fs.pathExists(destPath)) return;
    for (let chunkId of bcat.chunks) {
        if (! await fs.pathExists(`${fspath}/chunks/${chunkId}`)) return;
    }

    console.log("Saving BCAT: ", bcat.txId)
    for (let chunkId of bcat.chunks) {
        await streamFile(destPath, `${fspath}/chunks/${chunkId}`, { flags: 'a' })
    }

    const contentType = bcat.fileData.contentType;
    const hash = await createCLink(destPath);
    await saveMetadata(`bcat/${bcat.txId}`, contentType);
    await saveMetadata(`c/${hash}`, contentType);
}

async function processTransaction(txn) {
    const opRet = txn.out.find((out) => out.b0.op == 106);
    if (!opRet) return;
    let bcat;
    try {
        switch (opRet.s1) {
            case B:
                console.log(`Processing B: ${txn.tx.h}`);
                return saveB(txn.tx.h, opRet);
                break;

            case BCAT:
                console.log(`Processing BCAT: ${txn.tx.h}`);
                bcat = {
                    txId: txn.tx.h,
                    chunks: [],
                    fileData: {
                        info: opRet.s2,
                        contentType: opRet.s3,
                        encoding: opRet.s4,
                        filename: opRet.s5
                    }
                };

                let i = 7;
                let chunkId;
                while (chunkId = opRet[`h${i}`]) {
                    bcat.chunks.push(chunkId);
                    i++;
                }

                console.log(bcat);
                return saveBCat(bcat);
                break;

            case BCHUNK:
                console.log(`Processing Chunk: ${txn.tx.h}`);
                await saveChunk(txn.tx.h, opRet);
                break;

            default:
                return;
        }
    }
    catch (e) {
        return;
    }
}

planaria.start({
    filter: {
        from: 585000,
        q: {
            find: {
                "out.s1": { $in: [B, BCAT, BCHUNK] }
            }
        }
    },
    onstart: async (e) => {
        await fs.ensureDir(fspath + "/chunks");
        await fs.ensureDir(fspath + "/c");
        await fs.ensureDir(fspath + "/b");
        await fs.ensureDir(fspath + "/bcat");
    },
    onmempool: async (e) => {
        try {
            return processTransaction(e.tx)
        }
        catch (err) {
            console.log(err);
            process.exit()
        }
    },
    onblock: async (e) => {
        console.log("## onblock", "block height = ", e.height);
        try {
            for (let tx of e.tx) {
                await processTransaction(tx);
            }
            for (let tx of e.mem) {
                await processTransaction(tx);
            }
        }
        catch (err) {
            console.log(err);
            process.exit()
        }
    }
})

planarium.start({
    name: 'neon-file-server',
    port: 8080,
    onstart: async function () {},
    onquery: function (e) {},
    custom: function (e) {
        e.app.use(express.static(fspath, {
            setHeaders: (res, path, stat) => {
                let relPath = path.substr(fullpath.length);
                console.log(`PATH: ${relPath}`)
                let txn = en.beginTxn();
                let contentType = txn.getString(db, relPath);
                txn.commit();
                if(contentType) {
                    res.set('Content-Type', contentType);
                }
            }
        }));
    },
})