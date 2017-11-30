package com.dxhy.solr.service.impl;

import com.dxhy.entity.Invoice4Solr;
import com.dxhy.solr.dao.BaseSolrDao;
import com.dxhy.solr.dao.ClusterSolrDao;
import com.dxhy.solr.service.DXHYSolrSaveService;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by thinkpad on 2017/3/17.
 */

@Service
public class DXHYSolrSaveServiceImpl extends BaseSolrDao implements DXHYSolrSaveService {

    public List<Invoice4Solr> invoice4Solrs = new ArrayList<Invoice4Solr>();

    public static String OLD_COLLECTION;

    public static CloudSolrServer oldSaveServer;

    public boolean saveSolrInput(Invoice4Solr invoice4Solr) throws SolrServerException, IOException {

        String kprq = String.valueOf(invoice4Solr.getKPRQ());
        if ((ClusterSolrDao.COLLECTION_PREFIX + kprq).compareTo(ClusterSolrDao.COLLECTION) < 0) {

            if (oldSaveServer == null) {
                synchronized (DXHYSolrSaveServiceImpl.class) {
                    if (oldSaveServer == null) {
                        oldSaveServer = new CloudSolrServer(ClusterSolrDao.ZK_SERVER, false);
                        //通过kprq查找collection
                        oldSaveServer.setDefaultCollection(getCollection(kprq));
                    }
                }
            }
            oldSaveServer.addBean(invoice4Solr);

        } else {
            ClusterSolrDao.saveServer.addBean(invoice4Solr);
            if (oldSaveServer != null) {
                oldSaveServer = null;
            }
        }

        return true;

    }

}
