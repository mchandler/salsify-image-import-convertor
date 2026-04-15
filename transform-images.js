#!/usr/bin/env node
'use strict';

const fs = require('fs');
const path = require('path');
const { Transform } = require('stream');
const { parse } = require('csv-parse');
const { stringify } = require('csv-stringify');

const args = process.argv.slice(2);
if (args.length < 1) {
  console.error('Usage: node transform-images.js <input.csv>');
  process.exit(1);
}

const inputArg = args[0];
const inputPath = path.resolve(process.cwd(), inputArg);

const parsedName = path.parse(inputArg);
const outputName = `${parsedName.name}-output${parsedName.ext || '.csv'}`;
const outputPath = path.resolve(process.cwd(), outputName);

const LITERAL_ESCAPE_RE = /\\x[0-9a-fA-F]{2}/g;
function clean(value) {
  if (value == null) return '';
  return String(value).replace(LITERAL_ESCAPE_RE, '');
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

let sourceRowCount = 0;
let outputRowCount = 0;
let skippedRowCount = 0;
let isFirstDataRow = true;

const parser = parse({
  bom: true,
  relax_column_count: true,
  skip_empty_lines: true,
});

const stringifier = stringify({
  header: true,
  columns: ['title', 'URL', 'SKU', 'imageType'],
  record_delimiter: '\n',
});

const readStream = fs.createReadStream(inputPath);
const writeStream = fs.createWriteStream(outputPath);

readStream.on('error', (err) => {
  console.error(`Failed to read input file: ${err.message}`);
  process.exit(1);
});
writeStream.on('error', (err) => {
  console.error(`Failed to write output file: ${err.message}`);
  process.exit(1);
});

stringifier.pipe(writeStream);

parser.on('readable', () => {
  let record;
  while ((record = parser.read()) !== null) {
    if (isFirstDataRow) {
      isFirstDataRow = false;
      continue;
    }
    sourceRowCount++;

    const sku = clean(record[0]);
    const title = clean(record[2]);
    const candidates = [record[3], record[4], record[5], record[6]].filter(isNonEmpty);

    if (candidates.length === 0 || !isNonEmpty(record[3]) && !isNonEmpty(record[4])) {
      skippedRowCount++;
      continue;
    }

    const primary = clean(candidates[0]).trim();
    stringifier.write({ title, URL: primary, SKU: sku, imageType: 'productListImage' });
    stringifier.write({ title, URL: primary, SKU: sku, imageType: 'productDetailImage' });
    outputRowCount += 2;

    for (let k = 1; k < candidates.length; k++) {
      stringifier.write({
        title,
        URL: clean(candidates[k]).trim(),
        SKU: sku,
        imageType: 'productDetailImage',
      });
      outputRowCount++;
    }
  }
});

parser.on('error', (err) => {
  console.error(`CSV parse error: ${err.message}`);
  process.exit(1);
});

parser.on('end', () => {
  stringifier.end();
});

writeStream.on('finish', () => {
  console.log('Done.');
  console.log(`  Source rows processed: ${sourceRowCount}`);
  console.log(`  Output rows written:   ${outputRowCount}`);
  console.log(`  Rows skipped:          ${skippedRowCount}`);
  console.log(`  Output file:           ${outputPath}`);
});

readStream.pipe(stripHighBytes).pipe(parser);
