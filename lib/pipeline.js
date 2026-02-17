import { sparqlEscapeUri, sparqlEscapeString,sparqlEscapeDateTime, uuid } from "mu";
import { STATUS_BUSY, STATUS_SUCCESS, STATUS_FAILED, DEFAULT_GRAPH } from "../constant";
import { querySudo as query, updateSudo as update } from "@lblod/mu-auth-sudo";
import {
  loadTask,
  updateTaskStatus,
  appendTaskError,
  getHarvestCollectionForTask,
  getRemoteDataObjects,
  getPreviousJobsWithDiffResultsContainer,
  getPreviousIntersectionAndNewInserts,
  updateOldJobStatus
} from "./task";
const connectionOptions = {
  mayRetry: true
};



export async function run(deltaEntry) {
  const task = await loadTask(deltaEntry);
  if (!task) return;
  try {
    updateTaskStatus(task, STATUS_BUSY);
    const graphContainer = { id: uuid() };
    graphContainer.uri = `http://redpencil.data.gift/id/dataContainers/${graphContainer.id}`;
    const fileContainer = { id: uuid() };
    fileContainer.uri = `http://redpencil.data.gift/id/dataContainers/${fileContainer.id}`;
    const collectGraph = { id: uuid() };
    collectGraph.uri = `http://mu.semte.ch/graphs/harvesting/tasks/collect-server-info/${task.id}`;
    const collection = await getHarvestCollectionForTask(task);
    const rdo = await getRemoteDataObjects(collection);
    for (const { targetUrl } of rdo) {
      const previousJobsWithDiffResultsContainer  = await getPreviousJobsWithDiffResultsContainer(targetUrl);
      if(!previousJobsWithDiffResultsContainer?.length) {
        throw new Error(`no previous job found for target url ${targetUrl}`);
      }
      // first job is the most recent
      const {diffResultsContainer} =  previousJobsWithDiffResultsContainer[0];
      // fetch all files
      const previousFiles = await getPreviousIntersectionAndNewInserts(diffResultsContainer);

      // create a new logical file set "to be removed"

      for(const {file} of previousFiles) {
        const id = uuid();
        const now = new Date();
        const fileUri = `http://data.lblod.info/id/files/${id}`;

        const queryStr = `
  PREFIX dcterms: <http://purl.org/dc/terms/>
  PREFIX prov:    <http://www.w3.org/ns/prov#>
  INSERT {
    GRAPH <${DEFAULT_GRAPH}> {
      <${fileUri}>
          a ?type ;
          dcterms:created  ${sparqlEscapeDateTime(now)} ;
          dcterms:modified ${sparqlEscapeDateTime(now)} ;
          <http://mu.semte.ch/vocabularies/core/uuid> ${sparqlEscapeString(id)} ;
          dcterms:creator  <http://lblod.data.gift/services/cleanup-single-job-service> ;
          <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#fileName> "to-remove-triples.ttl" ;
          <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#fileSize> ?fileSize ;
          <http://dbpedia.org/ontology/fileExtension> ?extension ;
          dcterms:format ?format ;
          prov:wasDerivedFrom ?derivedFrom .
    }
  }
  WHERE {
    GRAPH <${DEFAULT_GRAPH}> {
      <${file}>
          a ?type ;
          <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#fileSize> ?fileSize ;
          <http://dbpedia.org/ontology/fileExtension> ?extension ;
          dcterms:format ?format ;
          prov:wasDerivedFrom ?derivedFrom .
    }
  }

        `;
        await update(queryStr, {}, connectionOptions);
      }

      await appendTaskResultFile(task, fileContainer, fileUri);
      await appendTaskResultFile(task, collectGraph, fileUri);
      // update jobs, mark as "ARCHIVED"
      // todo, must handle this status in the cleanup job
      for(const {job} of previousJobsWithDiffResultsContainer) {
        await updateOldJobStatus(job);
      }
    }



    updateTaskStatus(task, STATUS_SUCCESS);
  } catch (e) {
    console.error(e);
    if (task) {
      await appendTaskError(task, e.message);
      await upateTaskStatus(task, STATUS_FAILED);
    }
  }
}
async function appendTaskResultFile(task, container, fileUri) {
  // prettier-ignore
  const queryStr = `
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
    PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    INSERT DATA {
       GRAPH <${DEFAULT_GRAPH}> {
        ${sparqlEscapeUri(container.uri)} a nfo:DataContainer.
        ${sparqlEscapeUri(container.uri)} mu:uuid ${sparqlEscapeString(container.id)}.
        ${sparqlEscapeUri(container.uri)} task:hasFile ${sparqlEscapeUri(fileUri)}.
        ${sparqlEscapeUri(task.task)} task:resultsContainer ${sparqlEscapeUri(container.uri)}.
      }
    }
  `;

  await update(queryStr, {}, connectionOptions);
}
async function appendTaskResultGraph(task, container, graphUri) {
  // prettier-ignore
  const queryStr = `
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
    PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    INSERT DATA {
       GRAPH <${DEFAULT_GRAPH}> {
        ${sparqlEscapeUri(container.uri)} a nfo:DataContainer.
        ${sparqlEscapeUri(container.uri)} mu:uuid ${sparqlEscapeString(container.id)}.
        ${sparqlEscapeUri(container.uri)} task:hasGraph ${sparqlEscapeUri(graphUri)}.

        ${sparqlEscapeUri(task.task)} task:resultsContainer ${sparqlEscapeUri(container.uri)}.
      }
    }
  `;

  await update(queryStr, connectionOptions);
}
