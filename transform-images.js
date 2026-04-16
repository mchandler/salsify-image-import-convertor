#!/usr/bin/env node
'use strict';

const fs = require('fs');
const path = require('path');
const { Transform } = require('stream');
const { parse } = require('csv-parse');
const { stringify } = require('csv-stringify');

const args = process.argv.slice(2);
if (args.length < 1) {
  console.error('Usage: node transform-images.js <input.csv> [--chunk <rows>]');
  process.exit(1);
}

const inputArg = args[0];
const inputPath = path.resolve(process.cwd(), inputArg);
const parsedName = path.parse(inputArg);

let chunkSize = 0;
const chunkIdx = args.indexOf('--chunk');
if (chunkIdx !== -1 && args[chunkIdx + 1]) {
  chunkSize = parseInt(args[chunkIdx + 1], 10);
  if (isNaN(chunkSize) || chunkSize <= 0) {
    console.error('--chunk value must be a positive number');
    process.exit(1);
  }
}

const COLUMNS = ['title', 'url', 'SKU', 'imageType'];

const LITERAL_ESCAPE_RE = /\\x[0-9a-fA-F]{2}/g;
function clean(value) {
  if (value == null) return '';
  return String(value).replace(LITERAL_ESCAPE_RE, '').replace(/^http:\/\//i, 'https://');
}

const stripHighBytes = new Transform({
  transform(chunk, _enc, cb) {
    const out = Buffer.alloc(chunk.length);
    let j = 0;
    for (let i = 0; i < chunk.length; i++) {
      const b = chunk[i];
      if (b < 0x80) out[j++] = b;
    }
    cb(null, out.slice(0, j));
  },
});

function isNonEmpty(value) {
  return value != null && String(value).trim() !== '';
}

let fileIndex = 1;
let currentStringifier = null;
let currentWriteStream = null;
let currentFileRowCount = 0;
const outputFiles = [];

function openOutputFile() {
  if (currentStringifier) {
    currentStringifier.end();
  }
  const suffix = chunkSize > 0 ? `-${fileIndex}` : '';
  const name = `${parsedName.name}-output${suffix}${parsedName.ext || '.csv'}`;
  const filePath = path.resolve(process.cwd(), name);
  outputFiles.push(filePath);

  currentWriteStream = fs.createWriteStream(filePath);
  currentWriteStream.on('error', (err) => {
    console.error(`Failed to write output file: ${err.message}`);
    process.exit(1);
  });

  currentStringifier = stringify({
    header: true,
    columns: COLUMNS,
    record_delimiter: '\n',
  });
  currentStringifier.pipe(currentWriteStream);
  currentFileRowCount = 0;
  fileIndex++;
}

function writeRow(row) {
  if (!currentStringifier || (chunkSize > 0 && currentFileRowCount >= chunkSize)) {
    openOutputFile();
  }
  currentStringifier.write(row);
  currentFileRowCount++;
  outputRowCount++;
}

let sourceRowCount = 0;
let outputRowCount = 0;
let skippedRowCount = 0;
let isFirstDataRow = true;

const parser = parse({
  bom: true,
  relax_column_count: true,
  skip_empty_lines: true,
});

const readStream = fs.createReadStream(inputPath);

readStream.on('error', (err) => {
  console.error(`Failed to read input file: ${err.message}`);
  process.exit(1);
});

parser.on('readable', () => {
  let record;
  while ((record = parser.read()) !== null) {
    if (isFirstDataRow) {
      isFirstDataRow = false;
      continue;
    }
    sourceRowCount++;

    const sku = clean(record[0]);
    let title = clean(record[2]).replace(/"/g, 'in').replace(/'/g, 'ft').replace(/[,;.]/g, '').trim();
    if (!title) title = `${sku}-Image`;
    const candidates = [record[3], record[4], record[5], record[6]].filter(isNonEmpty);

    if (candidates.length === 0 || !isNonEmpty(record[3]) && !isNonEmpty(record[4])) {
      skippedRowCount++;
      continue;
    }

    const primary = clean(candidates[0]).trim();
    writeRow({ title, url: primary, SKU: sku, imageType: 'productListImage' });
    writeRow({ title, url: primary, SKU: sku, imageType: 'productDetailImage' });

    for (let k = 1; k < candidates.length; k++) {
      writeRow({
        title,
        url: clean(candidates[k]).trim(),
        SKU: sku,
        imageType: 'productDetailImage',
      });
    }
  }
});

parser.on('error', (err) => {
  console.error(`CSV parse error: ${err.message}`);
  process.exit(1);
});

parser.on('end', () => {
  if (currentStringifier) {
    currentStringifier.end();
  }
});

let filesFinished = 0;
function checkDone() {
  filesFinished++;
  if (filesFinished === outputFiles.length) {
    console.log('Done.');
    console.log(`  Source rows processed: ${sourceRowCount}`);
    console.log(`  Output rows written:   ${outputRowCount}`);
    console.log(`  Rows skipped:          ${skippedRowCount}`);
    console.log(`  Output files:          ${outputFiles.length}`);
    outputFiles.forEach((f) => console.log(`    ${f}`));
  }
}

parser.on('end', () => {
  if (outputFiles.length === 0) {
    console.log('Done. No output rows produced.');
    return;
  }
  outputFiles.forEach((f) => {
    const ws = fs.createWriteStream(f, { flags: 'r' });
    ws.on('error', () => {});
    ws.close();
  });
});

const origOpen = openOutputFile;
openOutputFile = function () {
  origOpen();
  currentWriteStream.on('finish', checkDone);
};

readStream.pipe(stripHighBytes).pipe(parser);
