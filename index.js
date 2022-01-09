const { MongoClient } = require('mongodb');
const AWS = require('aws-sdk');
//const PATH = require('path');
const fetch = require('node-fetch');

//contracts ABIs. These may need to change
//when contracts are updated. TODO: move these to external files
const CAMPAIGN_ABI = [{"inputs":[{"internalType":"uint256","name":"maxAmount","type":"uint256"},{"internalType":"address payable","name":"beneficiary","type":"address"},{"internalType":"address","name":"currency","type":"address"},{"internalType":"contract HEODAO","name":"dao","type":"address"},{"internalType":"uint256","name":"heoLocked","type":"uint256"},{"internalType":"uint256","name":"heoPrice","type":"uint256"},{"internalType":"uint256","name":"heoPriceDecimals","type":"uint256"},{"internalType":"uint256","name":"fee","type":"uint256"},{"internalType":"uint256","name":"feeDecimals","type":"uint256"},{"internalType":"address","name":"heoAddr","type":"address"},{"internalType":"string","name":"metaData","type":"string"}],"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"previousOwner","type":"address"},{"indexed":true,"internalType":"address","name":"newOwner","type":"address"}],"name":"OwnershipTransferred","type":"event"},{"inputs":[],"name":"owner","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"newOwner","type":"address"}],"name":"transferOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"stateMutability":"payable","type":"receive"},{"inputs":[],"name":"donateNative","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"donateERC20","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"currency","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"maxAmount","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"raisedAmount","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"heoPrice","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"heoPriceDecimals","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"fee","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"feeDecimals","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"isActive","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"beneficiary","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"heoLocked","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"metaData","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"string","name":"newMetaData","type":"string"}],"name":"updateMetaData","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"newMaxAmount","type":"uint256"},{"internalType":"string","name":"newMetaData","type":"string"}],"name":"update","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"newMaxAmount","type":"uint256"}],"name":"updateMaxAmount","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"close","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"renounceOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"}];
const ERC20_ABI = [{"inputs":[{"internalType":"string","name":"name_","type":"string"},{"internalType":"string","name":"symbol_","type":"string"}],"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"address","name":"spender","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Transfer","type":"event"},{"inputs":[],"name":"name","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"symbol","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"decimals","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"totalSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"account","type":"address"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"transfer","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"owner","type":"address"},{"internalType":"address","name":"spender","type":"address"}],"name":"allowance","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"approve","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"sender","type":"address"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"transferFrom","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"addedValue","type":"uint256"}],"name":"increaseAllowance","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"subtractedValue","type":"uint256"}],"name":"decreaseAllowance","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"}];
//require('dotenv').config({path : PATH.resolve(process.cwd(), '.env')});
var Web3 = require('web3');

var lastCheckedBlock = {bsc: 0, celo: 0, aurora: 0, eth: 0};
var updateS3 = {bsc: false, celo: false, aurora: false, eth: false};
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
        if(typeof jsonData.lastCheckedBlock == "object" && jsonData.lastCheckedBlock["bsc"]) {
            console.log("Found lastCheckedBlock")
            lastCheckedBlock = jsonData.lastCheckedBlock;
        } else {
            console.log("lastCheckedBlock is not an object");
            console.log(jsonData);
        }

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
    
    for(let i = 0; i < campaigns.length; i++) {
        let campaign = campaigns[i];
        console.log(`Checking raised amount for campaign ${campaign._id}`);
        if(campaign.addresses) {
            var campaignRaised = 0;
            var lastDonationTime = undefined;
            var lastDonationTS = 0;
            for(let chain in campaign.addresses) {
                var web3Provider = new Web3.providers.HttpProvider(process.env[chain+"_WEB3_RPC_NODE_URL"]);
                var web3 = new Web3(web3Provider);

                console.log(`Checking amount of ${campaign.coins[chain].name} raised on ${chain} for ${campaign._id}`);
                var campaignInstance = new web3.eth.Contract(CAMPAIGN_ABI, campaign.addresses[chain]);
                var coinInstance = new web3.eth.Contract(ERC20_ABI, campaign.coins[chain].address);
                var raisedWei = await campaignInstance.methods.raisedAmount().call();
                var decimals = await coinInstance.methods.decimals().call();
                var amountRaised = raisedWei / Math.pow(10, decimals);
                campaignRaised+=amountRaised;
            }

            if(campaignRaised != campaign.raisedAmount) {
                console.log(`Updating raised amount from ${campaign.raisedAmount} to ${campaignRaised} for campaign ${campaign._id}`);
                for(let chain in campaign.addresses) {
                    console.log(`Checking for transactions for campaign ${campaign._id} on ${chain} at ${campaign.addresses[chain]}`);
                    let fetchURL = `${process.env[(chain+"_API_BASE")]}/api?module=account&action=txlist&address=${campaign.addresses[chain]}&startblock=${lastCheckedBlock}&endblock=latest&sort=asc&apikey=${process.env[chain+"_API_KEY"]}`;
                    console.log(fetchURL);
                    let scanResult = await fetch(fetchURL);
                    let jsonResult = await scanResult.json();
                    if(jsonResult.result && jsonResult.result.length) {
                        console.log(`Found ${jsonResult.result.length} transactions for ${campaign.addresses[chain]}`);
                        for(let k = jsonResult.result.length-1; k >=0; k--) {
                            var txnObj = jsonResult.result[k];
                            if(txnObj.to == campaign.addresses[chain]) {
                                console.log(`Donation timestamp ${txnObj.timeStamp}`);
                                if(lastDonationTS < txnObj.timeStamp) {
                                    lastDonationTime = new Date(txnObj.timeStamp * 1000);
                                    lastDonationTS = txnObj.timeStamp;
                                }
                                lastCheckedBlock[chain] = txnObj.blockNumber;
                                updateS3[chain] = true;
                                console.log(`Found latest transaction for ${txnObj.to}. Last checked block is ${lastCheckedBlock[chain]}. lastDonationTime: ${lastDonationTime}`);
                                break;
                            } else {
                                console.log("unexpected transaction object");
                                console.log(txnObj);
                            }
                        }
                    } else {
                        console.log(`did not find any transactions for ${campaign._id}`)
                    }
                }
                if(lastDonationTime == undefined || lastDonationTime == 0) {
                    console.log("Did not find lastDonationTime");
                    lastDonationTime = new Date();
                }
                await DB.collection("campaigns").updateOne({ "_id" : campaign._id},
                    { "$set" : {"raisedAmount" : campaignRaised,"lastDonationTime" : lastDonationTime}});
                console.log(`Updated campaign ${campaign._id} with lastDonationTime ${lastDonationTime}`);
            }
        }


    }
    console.log("Updated donations");
    await CLIENT.close();

    var updateNeeded = false;
    var objForS3 = {"lastCheckedBlock":{}};
    for(let chain in updateS3) {
        if(updateS3[chain]) {
            updateNeeded = true;
            objForS3["lastCheckedBlock"][chain] = lastCheckedBlock[chain];
        }
    }
    if(updateNeeded) {
        params.Body = JSON.stringify(objForS3);
        params.ContentType = 'application/json';
        try {
            await S3.upload(params).promise();
            console.log(`File with latest block number uploaded successfully`);
        } catch (err) {
            console.log(err);
        }
    }
    console.log(`Worker is done`);
}
