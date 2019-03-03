const pubsub = require("./pubsub");
const postFileToAntivirusTopic = "postFileToAntivirus";
const {Storage} = require('@google-cloud/storage');
const apiBaseAddress = "https://api.metadefender.com/v2/file";
const rp = require('request-promise');
const apiKey = "aebbd440951a683271841a162df4c546";
const contentType = "application/octet-stream";
const projectId = 'qtg-poc-ea-project';
const bucketName = 'qtgeauploadfiles';
const authKeyPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;
const Firestore = require('@google-cloud/firestore');
const uuidv1 = require('uuid/v1');
const storage = new Storage({
    projectId: projectId
});
const fileMetadataCollectionName = "filemetadata";
const db = new Firestore(
    {
        projectId: projectId,
        keyFilename: authKeyPath
    });

function getScanProgressStatus(fileScanEvent) {
    const options = {
        headers: {
            "apikey": apiKey
        },
        uri: `${apiBaseAddress}/${fileScanEvent.data_id}`,
        method: 'GET'
    };
     rp(options).then(body => {
        let bodyObj = JSON.parse(body);
        const percentage = bodyObj.process_info.progress_percentage;
        fileScanEvent.scan_status = percentage;
	const collection = db.collection(fileMetadataCollectionName);
 	 const documentKey = uuidv1();//`${fileScanEvent.data_id} - ${fileScanEvent.scan_status}`;
	collection.doc(documentKey).set(fileScanEvent).then(data => {
	console.log(`${fileScanEvent} saved calling get percentage`);
	    });
    }).catch(err => console.log(`${err} when request to antivirus api`));
}
function pushFileToAntivirus(fileData) {
    const options = {
        headers: {
            "apikey": apiKey,
            "content-type": contentType
        },
        uri: apiBaseAddress,
        body: fileData,
        method: 'POST'
    };
    return rp(options);
}

exports.uploadEventProcessor = (event, context) => {
    const resource = context.resource;
    console.log('Function triggered by change to: ' +  resource);
    console.log(JSON.stringify(event));
    const collection = db.collection(fileMetadataCollectionName);
    let errorMessage = event.value.fields.errorMessage;
    if(errorMessage) {
	console.log("error should be notified to other listeners");
	return;
    }
    let errorEvent = {
	errorMessage: ""
    };
    let fileName = event.value.fields.fileName.stringValue || "";
    let	data_id = event.value.fields.data_id.stringValue || "";
    let scan_status = event.value.fields.scan_status.integerValue ||  event.value.fields.scan_status.stringValue || -1;
    if(!scan_status) {
	errorEvent.errorMessage += ` invalid value of scan_status == ${scan_status} `;
    }
    let accountId = event.value.fields.account_id.stringValue ||  "";
    if(!accountId) {
	errorEvent.errorMessage += ` invalid value of account_id  == ${accountId} `;
    }
    let document_type= event.value.fields.document_type.stringValue ||  "";
    if(!document_type) {
	errorEvent.errorMessage += ` invalid value of document_type  == ${document_type} `;
    }
    let document_name = event.value.fields.document_name.stringValue ||  "";
    if(!document_name) {
	errorEvent.errorMessage += ` invalid value of document_name  == ${document_name} `;
    }
    let user_id = event.value.fields.user_id.stringValue ||  "";
    if(!user_id) {
	errorEvent.errorMessage += ` invalid value of user_id  == ${user_id} `;
    }

    if(errorEvent.errorMessage) {
	console.log(errorEvent.errorMessage);
	console.log(`path ==== ${fileMetadataCollectionName}/${uuidv1()}`);
	collection.doc(`${uuidv1()}`).set(errorEvent).then(() => console.log("error event saved in firestore"))
	    .catch(err => `error when saving error event to firestore == ${err}`);
	return;
    }
    let fileScanEvent = {
	fileName: fileName,
	data_id: data_id,
	scan_status: scan_status,
	account_id: accountId,
	document_type: document_type,
	document_name: document_name,
	user_id: user_id
    };
   
    if(scan_status == 100) {
	console.log("finished scanning need to pass to laserfiche");
	return;
    }
    if(data_id) {
	if(fileScanEvent.scan_status !== "" && fileScanEvent.scan_status < 100) {
	    getScanProgressStatus(fileScanEvent);
	}
	console.log("data_id exists and scan status === " + scan_status);
    }
    else {
	let buf = "";
	var stream = storage.bucket(bucketName).file(fileName).createReadStream();
	stream.on('data', function (data) {
        buf += data;
    }).on("end", function () {
        let buffer = Buffer.from(buf, 'binary');
        console.log(buffer);
        pushFileToAntivirus(buffer, fileName).then(res => {
            const resultObject = JSON.parse(res);
            console.log(res);
            const data_id = resultObject.data_id;
    	    const documentKey = uuidv1();//`${data_id} - ${scan_status}`;
	    fileScanEvent.data_id = data_id;
            console.log(fileScanEvent.data_id);
	    collection.doc(documentKey).set(fileScanEvent).then(d => console.log("data save to firestore"))
	        .catch(err => console.log(`error when saving to firestore ${err}`));
        });
     });
    }
};

