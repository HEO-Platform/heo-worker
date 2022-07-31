const PATH = require('path');
require('dotenv').config({path : PATH.resolve(process.cwd(), '.env')});

const handler = require('./index.js');

const main = async() => {
    console.log("Calling handler");
    await handler.handler();
}

main();

