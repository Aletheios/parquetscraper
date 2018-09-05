#!/usr/bin/env node

'use strict';

/* eslint-disable no-console */

const fs = require('fs');
const parquet = require('parquetjs');
const jsome = require('jsome');
const isPlainObject = require('is-plain-object');
const { argv } = require('yargs');

function toFormattedJSON(object) {
    return JSON.stringify(object, null, 4);
}

function transformBuffers(row) {
    Object.keys(row).forEach(key => {
        if (Buffer.isBuffer(row[key])) {
            row[key] = String.fromCharCode.apply(null, row[key]);
        }
        else if (isPlainObject(row[key])) {
            transformBuffers(row[key]);
        }
    });
}

async function read(reader, { from, to }, writeStream = undefined) {
    const cursor = reader.getCursor();
    let isFirstRow = true;
    let index = 0;

    if (writeStream) {
        writeStream.write('[');
    }

    for (;;) {
        let row;
        try {
            row = await cursor.next();
        }
        catch (e) {
            console.error('ERROR: Cannot read row. ' + e);
            break;
        }

        if (!row) {
            break;
        }
        if (!(index >= from && index <= to)) {
            ++index;
            continue;
        }

        transformBuffers(row);
        if (writeStream) {
            if (!isFirstRow) {
                writeStream.write(', ');
            }
            writeStream.write(toFormattedJSON(row));
        }
        else {
            jsome(row);
        }

        ++index;
        isFirstRow = false;
    }

    if (writeStream) {
        writeStream.write(']');
    }
}

async function executeCommand(command, reader, options) {
    const { exportFile } = options;

    switch (command) {
        case 'read':
            let writeStream;
            if (exportFile) {
                writeStream = fs.createWriteStream(exportFile);
            }
            await read(reader, options, writeStream);
            if (writeStream) {
                writeStream.end();
            }
            break;

        case 'schema':
            const schema = reader.getSchema();
            if (exportFile) {
                fs.writeFileSync(exportFile, toFormattedJSON(schema), 'utf-8');
            }
            else {
                jsome(schema);
            }
            break;

        case 'rows':
            console.info(+reader.getRowCount());
            break;
            
        default:
            console.error('ERROR: Unknown command!');
            break;
    }
}

function getExportFileName(file, command) {
    const filenameParts = file.split('.');
    filenameParts.pop();
    if (command === 'read') {
        command = 'contents';
    }
    return `${filenameParts.join('.')}.${command}.json`;
}

async function main() {
    if (!Array.isArray(argv._) || argv._.length !== 2) {
        console.error('ERROR: No command and/or filename specified!\nUsage: parquetscraper [command] [filename] [options]');
        return;
    }

    const [command, file] = argv._;
    const exportFile = argv.export ? getExportFileName(file, command) : undefined;
    const from = Math.max(0, +argv.from || 0);
    const to = Math.max(0, +argv.to || Infinity);
    if (from > to) {
        console.error('ERROR: Wrong row range orientation!');
        return;
    }

    let reader;
    try {
        reader = await parquet.ParquetReader.openFile(file);
    }
    catch (e) {
        console.error('ERROR: Cannot read the given file! ' + e);
        return;
    }
    
    await executeCommand(command, reader, { exportFile, from, to });
    reader.close();
}

main();