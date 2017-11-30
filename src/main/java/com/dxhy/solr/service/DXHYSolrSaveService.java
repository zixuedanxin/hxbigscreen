package com.dxhy.solr.service;

import com.dxhy.entity.Invoice4Solr;
import org.apache.solr.client.solrj.SolrServerException;

import java.io.IOException;

/**
 * Created by thinkpad on 2017/3/17.
 */
public interface DXHYSolrSaveService {

    boolean saveSolrInput(Invoice4Solr invoice4Solr) throws SolrServerException, IOException;
}
