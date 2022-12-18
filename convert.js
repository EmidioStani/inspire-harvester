const JsonLdParser = require("jsonld-streaming-parser").JsonLdParser;
fs = require('fs');
const ContextParser = require('jsonld-context-parser').ContextParser;
const FetchDocumentLoader = require('jsonld-context-parser').FetchDocumentLoader;
const myParser = new JsonLdParser();

function api(url, RequestInit = {}) {
  return fetch(url)
    .then(response => {
      if (!response.ok) {
        throw new Error(response.statusText)
      }
      response.headers.set("Content-Type","text/plain")
      return response
    })
}

const myParser2 = new ContextParser({
    documentLoader: new FetchDocumentLoader(api('https://raw.githubusercontent.com/SEMICeu/DCAT-AP/master/releases/2.1.0/dcat-ap_2.1.0.jsonld')),
    skipValidation: true,
    expandContentTypeToBase: true,
  });

var start = async function() { 
    // Your async task will execute with await
    const myContext = await myParser2.parse('https://raw.githubusercontent.com/SEMICeu/DCAT-AP/master/releases/2.1.0/dcat-ap_2.1.0.jsonld');
    console.log('I will execute after foo get either resolved/rejected')
  }
start()
