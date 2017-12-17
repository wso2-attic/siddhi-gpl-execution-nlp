siddhi-gpl-execution-nlp
======================================

The **siddhi-gpl-execution-nlp extension** is an extension to <a target="_blank" href="https://wso2.github.io/siddhi">Siddhi
</a> that  can be used to process natural language.

Find some useful links below:

* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-gpl-execution-nlp">Source code</a>
* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-gpl-execution-nlp/releases">Releases</a>
* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-gpl-execution-nlp/issues">Issue tracker</a>

## Latest API Docs 

Latest API Docs is <a target="_blank" href="https://wso2-extensions.github.io/siddhi-gpl-execution-nlp/api/4.0.7">4.0.7</a>.

## How to use 

**Using the extension in <a target="_blank" href="https://github.com/wso2/product-sp">WSO2 Stream Processor</a>**

* You can use this extension in the latest <a target="_blank" href="https://github.com/wso2/product-sp/releases">WSO2 Stream Processor</a> that is a part of <a target="_blank" href="http://wso2.com/analytics?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">WSO2 Analytics</a> offering, with editor, debugger and simulation support. 

* You can use  this extension after copying the component <a target="_blank" href="https://maven.wso2.org/nexus/content/groups/wso2-public/org/wso2/extension/siddhi/gpl/execution/nlp/siddhi-gpl-execution-nlp/">jar</a> to the `<STREAM_PROCESSOR_HOME>/lib` directory.

**Using the extension as a <a target="_blank" href="https://wso2.github.io/siddhi/documentation/running-as-a-java-library">java library</a>**

* This extension can be added as a maven dependency along with other Siddhi dependencies to your project.

```
     <dependency>
        <groupId>org.wso2.extension.siddhi.gpl.execution.nlp</groupId>
        <artifactId>siddhi-gpl-execution-nlp</artifactId>
        <version>x.x.x</version>
     </dependency>
```

## Jenkins Build Status

---

|  Branch | Build Status |
| :------ |:------------ | 
| master  | [![Build Status](https://wso2.org/jenkins/job/siddhi/job/siddhi-gpl-execution-nlp/badge/icon)](https://wso2.org/jenkins/job/siddhi/job/siddhi-gpl-execution-nlp/) |

---

## Features

* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-gpl-execution-nlp/api/4.0.7/#findnameentitytype-stream-processor">findNameEntityType</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#stream-processor">(Stream Processor)</a>)*<br><div style="padding-left: 1em;"><p>This feature finds the entities in the text by the given type.</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-gpl-execution-nlp/api/4.0.7/#findnameentitytypeviadictionary-stream-processor">findNameEntityTypeViaDictionary</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#stream-processor">(Stream Processor)</a>)*<br><div style="padding-left: 1em;"><p>This feature finds the entities in the text that have been defined in the dictionary.</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-gpl-execution-nlp/api/4.0.7/#findrelationshipbyregex-stream-processor">findRelationshipByRegex</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#stream-processor">(Stream Processor)</a>)*<br><div style="padding-left: 1em;"><p>This feature extracts subject, object and verb from the text stream that matches the named nodes of the Semgrex pattern.</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-gpl-execution-nlp/api/4.0.7/#findrelationshipbyverb-stream-processor">findRelationshipByVerb</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#stream-processor">(Stream Processor)</a>)*<br><div style="padding-left: 1em;"><p>This feature extracts the subject, object, and verb relationship for a given verb base form.</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-gpl-execution-nlp/api/4.0.7/#findsemgrexpattern-stream-processor">findSemgrexPattern</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#stream-processor">(Stream Processor)</a>)*<br><div style="padding-left: 1em;"><p>This feature extracts named nodes (through the Semgrex pattern) from the text stream.</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-gpl-execution-nlp/api/4.0.7/#findtokensregexpattern-stream-processor">findTokensRegexPattern</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#stream-processor">(Stream Processor)</a>)*<br><div style="padding-left: 1em;"><p>This feature extracts groups (defined in the Semgrex pattern) from the text stream.</p></div>

## How to Contribute
 
  * Please report issues at <a target="_blank" href="https://github.com/wso2-extensions/siddhi-gpl-execution-nlp/issues">GitHub Issue Tracker</a>.
  
  * Send your contributions as pull requests to <a target="_blank" href="https://github.com/wso2-extensions/siddhi-gpl-execution-nlp/tree/master">master branch</a>. 
 
## Contact us 

 * Post your questions with the <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">"Siddhi"</a> tag in <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">Stackoverflow</a>. 
 
 * Siddhi developers can be contacted via the mailing lists:
 
    Developers List   : [dev@wso2.org](mailto:dev@wso2.org)
    
    Architecture List : [architecture@wso2.org](mailto:architecture@wso2.org)
 
## Support 

* We are committed to ensuring support for this extension in production. Our unique approach ensures that all support leverages our open development methodology and is provided by the very same engineers who build the technology. 

* For more details and to take advantage of this unique opportunity contact us via <a target="_blank" href="http://wso2.com/support?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">http://wso2.com/support/</a>. 
