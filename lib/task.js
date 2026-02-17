import { sparqlEscapeUri, sparqlEscapeString, sparqlEscapeDateTime, uuid,   } from "mu";
import { querySudo as query, updateSudo as update } from "@lblod/mu-auth-sudo";

import {
  TASK_TYPE,
  PREFIXES,
  STATUS_ARCHIVED,
  ERROR_URI_PREFIX,
  DEFAULT_GRAPH,
  TASK_TYPE_URI,
  ERROR_TYPE,
} from "../constant";
import { parseResult } from "./utils";

const connectionOptions = {
  mayRetry: true
};



export async function loadTask(subject) {
  const queryTask = `
   ${PREFIXES}
   SELECT DISTINCT ?task ?id ?job ?jobId ?created ?modified ?status ?index ?operation ?error WHERE {
      BIND(${sparqlEscapeUri(subject)} as ?task)
      ?task a ${sparqlEscapeUri(TASK_TYPE)}.
      ?task dct:isPartOf ?job;
                    mu:uuid ?id;
                    dct:created ?created;
                    dct:modified ?modified;
                    adms:status ?status;
                    task:index ?index;
                    task:inputContainer ?inputContainer;

                    task:operation ${sparqlEscapeUri(TASK_TYPE_URI)}.
       ?job mu:uuid ?jobId.

      OPTIONAL { ?task task:error ?error. }
   }
  `;

  const task = parseResult(await query(queryTask, connectionOptions))[0];

  return task;
}
export async function getHarvestCollectionForTask(task) {
  const queryStr = `
    PREFIX tasks: <http://redpencil.data.gift/vocabularies/tasks/>
    SELECT ?collection
    WHERE {
        <${task.task}> tasks:inputContainer ?inputContainer.
        ?inputContainer tasks:hasHarvestingCollection ?collection.
    }
    `;
  const collection = parseResult(await query(queryStr, connectionOptions));
  if (!collection?.length) {
    return null;
  }
  return collection[0];
}


export async function getPreviousIntersectionAndNewInserts(resultContainer) {
  const queryStr = `
  PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX cogs: <http://vocab.deri.ie/cogs#>
PREFIX adms: <http://www.w3.org/ns/adms#>
PREFIX jobst: <http://redpencil.data.gift/id/concept/JobStatus/>
PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
PREFIX tasko: <http://lblod.data.gift/id/jobs/concept/TaskOperation/>
PREFIX dlstatus: <http://lblod.data.gift/file-download-statuses/>
PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
PREFIX prov: <http://www.w3.org/ns/prov#>

SELECT DISTINCT ?file where {
    VALUES ?filename {
      "intersect-triples.ttl"
      "new-insert-triples.ttl"
    }
 	<${resultContainer}> task:hasFile ?file.
 	?file nfo:fileName ?filename.
 	?path nie:dataSource ?file.
}
  `;
    const res = parseResult(await query(queryStr, connectionOptions));
  return res;
}
export async function getPreviousJobsWithDiffResultsContainer(targetUrl) {
  const queryStr = `
PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX cogs: <http://vocab.deri.ie/cogs#>
PREFIX adms: <http://www.w3.org/ns/adms#>
PREFIX jobst: <http://redpencil.data.gift/id/concept/JobStatus/>
PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
PREFIX tasko: <http://lblod.data.gift/id/jobs/concept/TaskOperation/>
PREFIX dlstatus: <http://lblod.data.gift/file-download-statuses/>
PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
PREFIX prov: <http://www.w3.org/ns/prov#>

SELECT DISTINCT ?job ?diffResultsContainer ?modified
WHERE {
    ?task task:operation tasko:collecting ;
          task:inputContainer ?inputContainer .
    
    ?inputContainer task:hasHarvestingCollection ?collection .
    ?task dct:isPartOf ?job .
    ?collection dct:hasPart ?dataObject .
    
    ?dataObject a nfo:RemoteDataObject ;
                nie:url <${targetUrl}> .
    
    VALUES ?endOperation {
        tasko:publishHarvestedTriples
        tasko:publishHarvestedTriplesWithDeletes
    }
    
    ?taskPublishing dct:isPartOf ?job ;
                    task:operation ?endOperation ;
                    adms:status jobst:success .
    
    ?job dct:modified ?modified .
    
    ?taskDiff dct:isPartOf ?job ;
              task:operation tasko:diff ;
              adms:status jobst:success ;
              task:resultsContainer ?diffResultsContainer .
    
    ?diffResultsContainer task:hasFile ?file .
} order by desc(?modified)
 
 `;
 
  const res = parseResult(await query(queryStr, connectionOptions));
  return res;
}


export async function getRemoteDataObjects(collection) {
  const queryStr = `
    PREFIX    adms: <http://www.w3.org/ns/adms#>
    PREFIX    prov: <http://www.w3.org/ns/prov#>
    PREFIX    mu:   <http://mu.semte.ch/vocabularies/core/>
    PREFIX    nie:  <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
    PREFIX    dct:  <http://purl.org/dc/terms/>
    PREFIX    nfo:  <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
    PREFIX    nuao: <http://www.semanticdesktop.org/ontologies/2010/01/25/nuao#>
    PREFIX    ext:  <http://mu.semte.ch/vocabularies/ext/>
    SELECT DISTINCT ?dataObject ?targetUrl ?uuid
    WHERE {
        <${collection.collection}> dct:hasPart ?dataObject.
        ?dataObject a nfo:RemoteDataObject;
             mu:uuid ?uuid;
             nie:url ?targetUrl.
    }
`;
  const rdo = parseResult(await query(queryStr, connectionOptions));
  return rdo;
}
export async function updateOldJobStatus(jobUri) {
  await update(
    `
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX adms: <http://www.w3.org/ns/adms#>
    PREFIX dct: <http://purl.org/dc/terms/>
    DELETE {
      GRAPH ?g {
        ?subject adms:status ?status .
        ?subject dct:modified ?modified.
      }
    }
    INSERT {
      GRAPH ?g {
       ?subject adms:status ${sparqlEscapeUri(STATUS_ARCHIVED)}.
       ?subject dct:modified ${sparqlEscapeDateTime(new Date())}.
      }
    }
    WHERE {
      GRAPH ?g {
        BIND(${sparqlEscapeUri(jobUri)} as ?subject)
        ?subject adms:status ?status .
        OPTIONAL { ?subject dct:modified ?modified. }
      }
    }
  `,
    connectionOptions,
  );
}
export async function updateTaskStatus(task, status) {
  await update(
    `
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX adms: <http://www.w3.org/ns/adms#>
    PREFIX dct: <http://purl.org/dc/terms/>
    DELETE {
      GRAPH ?g {
        ?subject adms:status ?status .
        ?subject dct:modified ?modified.
      }
    }
    INSERT {
      GRAPH ?g {
       ?subject adms:status ${sparqlEscapeUri(status)}.
       ?subject dct:modified ${sparqlEscapeDateTime(new Date())}.
      }
    }
    WHERE {
      GRAPH ?g {
        BIND(${sparqlEscapeUri(task.task)} as ?subject)
        ?subject adms:status ?status .
        OPTIONAL { ?subject dct:modified ?modified. }
      }
    }
  `,
    connectionOptions,
  );
}

export async function appendTaskError(task, errorMsg) {
  const id = uuid();
  const uri = ERROR_URI_PREFIX + id;

  const queryError = `
   ${PREFIXES}
   INSERT DATA {
     GRAPH <${DEFAULT_GRAPH}> {
      ${sparqlEscapeUri(uri)} a ${sparqlEscapeUri(ERROR_TYPE)};
        mu:uuid ${sparqlEscapeString(id)};
        oslc:message ${sparqlEscapeString(errorMsg)}.
      ${sparqlEscapeUri(task.task)} task:error ${sparqlEscapeUri(uri)}.
     }
   }
  `;

  await update(queryError, connectionOptions);
}
