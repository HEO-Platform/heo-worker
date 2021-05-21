const PATH = require('path');
const { MongoClient } = require('mongodb');
const fetch = require('node-fetch');

//contracts ABIs. These may need to change
//when contracts are updated. TODO: move these to external files
const ERC20_ABI = [{"inputs":[{"internalType":"string","name":"name_","type":"string"},{"internalType":"string","name":"symbol_","type":"string"}],"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"address","name":"spender","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Transfer","type":"event"},{"inputs":[],"name":"name","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"symbol","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"decimals","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"totalSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"account","type":"address"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"transfer","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"owner","type":"address"},{"internalType":"address","name":"spender","type":"address"}],"name":"allowance","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"approve","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"sender","type":"address"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"transferFrom","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"addedValue","type":"uint256"}],"name":"increaseAllowance","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"subtractedValue","type":"uint256"}],"name":"decreaseAllowance","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"}];
const FACTORY_ABI = [{"inputs":[{"internalType":"contract HEODAO","name":"dao","type":"address"}],"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"campaignAddress","type":"address"},{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"address","name":"beneficiary","type":"address"},{"indexed":false,"internalType":"uint256","name":"maxAmount","type":"uint256"},{"indexed":false,"internalType":"address","name":"token","type":"address"},{"indexed":false,"internalType":"string","name":"metaUrl","type":"string"}],"name":"CampaignDeployed","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"previousOwner","type":"address"},{"indexed":true,"internalType":"address","name":"newOwner","type":"address"}],"name":"OwnershipTransferred","type":"event"},{"inputs":[],"name":"owner","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function","constant":true},{"inputs":[{"internalType":"address","name":"newOwner","type":"address"}],"name":"transferOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"maxAmount","type":"uint256"},{"internalType":"address","name":"token","type":"address"},{"internalType":"string","name":"metaUrl","type":"string"},{"internalType":"address payable","name":"beneficiary","type":"address"}],"name":"createCampaign","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"maxAmount","type":"uint256"},{"internalType":"address","name":"token","type":"address"},{"internalType":"string","name":"metaUrl","type":"string"},{"internalType":"address payable","name":"beneficiary","type":"address"}],"name":"createRewardCampaign","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"renounceOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"}];
const CAMPAIGN_ABI = [{"inputs":[{"internalType":"uint256","name":"maxAmount","type":"uint256"},{"internalType":"address payable","name":"beneficiary","type":"address"},{"internalType":"address","name":"currency","type":"address"},{"internalType":"string","name":"metaUrl","type":"string"},{"internalType":"contract HEODAO","name":"dao","type":"address"},{"internalType":"uint256","name":"heoLocked","type":"uint256"},{"internalType":"uint256","name":"heoPrice","type":"uint256"},{"internalType":"uint256","name":"heoPriceDecimals","type":"uint256"},{"internalType":"uint256","name":"fee","type":"uint256"},{"internalType":"uint256","name":"feeDecimals","type":"uint256"},{"internalType":"address","name":"heoAddr","type":"address"}],"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"previousOwner","type":"address"},{"indexed":true,"internalType":"address","name":"newOwner","type":"address"}],"name":"OwnershipTransferred","type":"event"},{"inputs":[],"name":"owner","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"newOwner","type":"address"}],"name":"transferOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"stateMutability":"payable","type":"receive"},{"inputs":[],"name":"donateNative","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"donateERC20","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"string","name":"metaDataUrl","type":"string"}],"name":"updateMetaDataUrl","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"currency","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"maxAmount","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"raisedAmount","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"heoPrice","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"heoPriceDecimals","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"fee","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"feeDecimals","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"isActive","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"beneficiary","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"heoLocked","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"metaDataUrl","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"close","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"renounceOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"}];

require('dotenv').config({path : PATH.resolve(process.cwd(), '.env')});

var Web3 = require('web3');
var web3Provider = new Web3.providers.HttpProvider(process.env.WEB3_RPC_NODE_URL);
var web3 = new Web3(web3Provider);
var lastCheckedBlock = 0;

//environment-specific globals
const FACTORY_ADDRESS = process.env.CAMPAIGN_FACTORY_ADDRESS.toLowerCase();
const URL = `mongodb+srv://${process.env.MONGO_LOGIN}:${process.env.MONGODB_PWD}${process.env.MONGO_URL}`;
const DBNAME = process.env.MONGO_DB_NAME;

async function doWork() {
    console.log(`Started worker`);
    process.on('SIGTERM', () => {
        console.log(`Worker ${id} exiting (cleanup here)`)
    })
    var factoryInstance = new web3.eth.Contract(
        FACTORY_ABI,
        FACTORY_ADDRESS,
    );

    //connect to mongo
    var CLIENT = new MongoClient(URL);
    await CLIENT.connect();
    var DB = CLIENT.db(DBNAME);

    //fetch campaigns since last block
    console.log("Fetching new campaigns");
    let events = await factoryInstance.getPastEvents("CampaignDeployed", { fromBlock: lastCheckedBlock, toBlock: 'latest' });
    console.log()
    for(let i=0; i < events.length; i++) {
        let eventBlock = events[i].blockNumber+1;
        let campaignAddress = events[i].returnValues.campaignAddress.toLowerCase();
        console.log(`Campaign ${campaignAddress} created in block ${eventBlock}`);
        //check if this campaign already exists
        let result = await DB.collection("campaigns").findOne({"_id" : campaignAddress});
        if(!result) {
            //add this campaign to the database
            let owner = events[i].returnValues.owner;
            let beneficiary = events[i].returnValues.beneficiary;
            let maxAmount = events[i].returnValues.maxAmount;
            let token = events[i].returnValues.token.toLowerCase();
            let metaUrl = events[i].returnValues.metaUrl;
            let metaFile = await fetch(metaUrl);
            let metaJSON = await metaFile.json();

            //get currency symbol
            console.log(`getting the name of coin at ${token}`);
            let coinInstance = new web3.eth.Contract(
                ERC20_ABI,
                token
            );

            let coinName = await coinInstance.methods.symbol().call();
            console.log(coinName);
            let date = Date.now();
            const ITEM = {
                _id : campaignAddress,
                beneficiaryId : beneficiary.toLowerCase(),
                ownerId : owner.toLowerCase(),
                title : metaJSON.title,
                mainImage : metaJSON.mainImageURL,
                videoLink : metaJSON.vl,
                campaignDesc : metaJSON.description,
                coinName: coinName,
                coinAddress: token,
                maxAmount: web3.utils.fromWei(maxAmount),
                raisedAmount: "0",
                creationDate : date,
            }
            const DB = CLIENT.db(DBNAME);
            await DB.collection('campaigns').insertOne(ITEM);
            console.log("Added campaign to the database");
        } else {
            console.log(`Campaign ${campaignAddress} already in the database`);
        }
        lastCheckedBlock = eventBlock;
    }
    console.log("Updated new campaigns");
    console.log("Updating donations");
    let campaigns = await DB.collection("campaigns").find().toArray();
    for(let i = 0; i < campaigns.length; i++) {
        let campaign = campaigns[i];
        var campaignInstance = new web3.eth.Contract(
            CAMPAIGN_ABI,
            campaign._id,
        );
        let amountRaised = await campaignInstance.methods.raisedAmount().call();
        if(web3.utils.fromWei(amountRaised) != campaign.raisedAmount) {
            console.log("Updating raised amount");
            await DB.collection("campaigns").findOneAndUpdate({ "_id" : campaign._id}, { "$set" : {"raisedAmount" : web3.utils.fromWei(amountRaised)}})
        }
    }
    console.log("Updated donations");
    console.log(`Worker is done`);
}