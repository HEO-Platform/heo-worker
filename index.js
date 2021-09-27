const { MongoClient } = require('mongodb');
const AWS = require('aws-sdk');
//const PATH = require('path');
const fetch = require('node-fetch');

//contracts ABIs. These may need to change
//when contracts are updated. TODO: move these to external files
const CAMPAIGN_ABI = [{"inputs":[{"internalType":"uint256","name":"maxAmount","type":"uint256"},{"internalType":"address payable","name":"beneficiary","type":"address"},{"internalType":"address","name":"currency","type":"address"},{"internalType":"contract HEODAO","name":"dao","type":"address"},{"internalType":"uint256","name":"heoLocked","type":"uint256"},{"internalType":"uint256","name":"heoPrice","type":"uint256"},{"internalType":"uint256","name":"heoPriceDecimals","type":"uint256"},{"internalType":"uint256","name":"fee","type":"uint256"},{"internalType":"uint256","name":"feeDecimals","type":"uint256"},{"internalType":"address","name":"heoAddr","type":"address"},{"internalType":"string","name":"metaData","type":"string"}],"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"previousOwner","type":"address"},{"indexed":true,"internalType":"address","name":"newOwner","type":"address"}],"name":"OwnershipTransferred","type":"event"},{"inputs":[],"name":"owner","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"newOwner","type":"address"}],"name":"transferOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"stateMutability":"payable","type":"receive"},{"inputs":[],"name":"donateNative","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"donateERC20","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"currency","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"maxAmount","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"raisedAmount","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"heoPrice","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"heoPriceDecimals","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"fee","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"feeDecimals","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"isActive","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"beneficiary","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"heoLocked","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"metaData","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"string","name":"newMetaData","type":"string"}],"name":"updateMetaData","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"newMaxAmount","type":"uint256"},{"internalType":"string","name":"newMetaData","type":"string"}],"name":"update","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"newMaxAmount","type":"uint256"}],"name":"updateMaxAmount","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"close","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"renounceOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"}];
//require('dotenv').config({path : PATH.resolve(process.cwd(), '.env')});
var Web3 = require('web3');
var web3Provider = new Web3.providers.HttpProvider(process.env.WEB3_RPC_NODE_URL);
var web3 = new Web3(web3Provider);
var lastCheckedBlock = 0;
var updateS3 = false;
//environment-specific globals
const URL = `mongodb+srv://${process.env.MONGO_LOGIN}:${process.env.MONGODB_PWD}${process.env.MONGO_URL}`;
const DBNAME = process.env.MONGO_DB_NAME;
const S3 = new AWS.S3({accessKeyId:process.env.SERVER_APP_ACCESS_ID,
    secretAccessKey:process.env.SERVER_APP_ACCESS_KEY});

exports.handler = async function(event, context) {
    console.log(`Started worker`);
     //connect to mongo
    var CLIENT = new MongoClient(URL);
    await CLIENT.connect();
    var DB = CLIENT.db(DBNAME);

    //read last checked block
    var params = {
        Bucket: process.env.STATE_BUCKET,
        Key: process.env.STATE_KEY,
    };
    try {
        const data = await S3.getObject(params).promise();
        const jsonData = JSON.parse(data.Body.toString('utf-8'));
        console.log(jsonData);
        lastCheckedBlock = jsonData.lastCheckedBlock;
    } catch (err) {
        if(err && err.code && err.code == 'NoSuchKey') {
            params.Body = JSON.stringify({lastCheckedBlock:lastCheckedBlock});
            params.ContentType = 'application/json';
            S3.upload(params, function(s3Err, data) {
                if (s3Err) throw s3Err
                console.log(`File uploaded successfully at ${data.Location}`)
            });

        } else {
            console.log(err);
        }
    }
    console.log("Updating donations");
    let campaigns = await DB.collection("campaigns").find().toArray();
    console.log(`Found ${campaigns.length} campaigns`);
    var lastBlock = await web3.eth.getBlockNumber();
    console.log(`Current block number ${lastBlock}`);

    console.log(`lastCheckedBlock ${lastCheckedBlock}`);
    for(let i = 0; i < campaigns.length; i++) {
        let campaign = campaigns[i];
        console.log(`Checking raised amount for campaign ${campaign._id}`);
        var campaignInstance = new web3.eth.Contract(
            CAMPAIGN_ABI,
            campaign._id,
        );
        let amountRaised = await campaignInstance.methods.raisedAmount().call();
        var lastDonationTime = undefined;
        if(web3.utils.fromWei(amountRaised) != campaign.raisedAmount) {
            console.log(`Updating raised amount for campaign ${campaign._id}`);
            let fetURL = `${process.env.BINANCE_API_BASE}/api?module=account&action=txlist&address=${campaign._id}&startblock=${lastCheckedBlock}&endblock=latest&sort=asc&apikey=${process.env.BINANCE_API_KEY}`;
            console.log(fetURL);
            let bscScan = await fetch(fetURL);
            let bscJSON = await bscScan.json();
            if(bscJSON.result && bscJSON.result.length) {
                console.log(`Found ${bscJSON.result.length} transactions for ${campaign._id}`);
                for(let k = bscJSON.result.length-1; k >=0; k--) {
                    var txnObj = bscJSON.result[k];
                    if(txnObj.to == campaign._id) {
                        lastDonationTime = new Date(txnObj.timestamp * 1000);
                        lastCheckedBlock = txnObj.blockNumber;
                        updateS3 = true;
                        console.log(`Found latest transaction for ${txnObj.to}. Last checked block is ${lastCheckedBlock}`);
                        break;
                    } else {
                        console.log("unexpected transaction object");
                        console.log(txnObj);
                    }
                }
            } else {
                console.log(`did not find any transactions for ${campaign._id}`)
            }

            if(lastDonationTime == undefined || lastDonationTime == 0) {
                console.log("Did not find lastDonationTime");
                lastDonationTime = new Date.now();
            }
            await DB.collection("campaigns").updateOne({ "_id" : campaign._id},
                { "$set" : {"raisedAmount" : web3.utils.fromWei(amountRaised),"lastDonationTime" : lastDonationTime}});
        }
    }
    console.log("Updated donations");
    await CLIENT.close();
    if(updateS3) {
        params.Body = JSON.stringify({lastCheckedBlock:lastCheckedBlock});
        params.ContentType = 'application/json';
        S3.upload(params, function(s3Err, data) {
            if (s3Err) throw s3Err
            console.log(`File uploaded successfully at ${data.Location}`)
        });
    }
    console.log(`Worker is done`);
}