package com.lucene.service;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TrackingIndexWriter;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BooleanQuery.Builder;
import org.apache.lucene.search.ControlledRealTimeReopenThread;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.lucene.config.LucenePropertiesConfigure;

@Component
public class LuceneNRTServiceImpl {
	@Autowired
	private LucenePropertiesConfigure properties;
	@Autowired
	private IndexWriter indexWriter;
	@Autowired
	private TrackingIndexWriter trackingIndexWriter;
	@Autowired
	private ReferenceManager<IndexSearcher> reMgr;
	@Autowired
	private ControlledRealTimeReopenThread<IndexSearcher> crt;
	private int fileCount = 0;
	private int fileNum = 1;
	private long targetGen;
	
	@Bean
	protected IndexWriter getIndexWriter() throws IOException {
		Directory directory = FSDirectory.open(new File(properties.getIndexNRTDir()).toPath());
		return new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer()));
	}
	
	@Bean
	protected TrackingIndexWriter getTrackingIndexWriter() {
		return new TrackingIndexWriter(indexWriter);
	}
	
	@Bean
	protected ReferenceManager<IndexSearcher> getReferenceManager() throws IOException {
		return new SearcherManager(indexWriter, true, null);
	}
	
	@Bean
	protected ControlledRealTimeReopenThread<IndexSearcher> getControlledRealTimeReopenThread() {
		ControlledRealTimeReopenThread<IndexSearcher> crt = new ControlledRealTimeReopenThread<IndexSearcher>(trackingIndexWriter, reMgr, 5.0, 0.025);
		crt.setDaemon(true);
		crt.setName("后台刷新服务");
		crt.start();
		return crt;
	}

	/**
	 * 定期提交内存中的索引到硬盘上，防止丢失
	 */
	public void commit() {
		try {
			indexWriter.commit();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 建立索引
	 * 要实现 search NRT, 需要使用TrackIndexWriter保存document, 同时Writer不需要关闭
	 * @param indexPath
	 */
	public void doIndex(File indexPath) {
		try {
			getFileCount(indexPath);
			addDocument(indexPath);
			System.out.println("\nIndex finished.");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// 首次创建，提交索引。只有提交后，才会在索引片段中也将信息改变
			try {
				commit();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public void query() {
		try {
			crt.waitForGeneration(targetGen);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		IndexSearcher indexSearcher = getSearcher();
		System.out.println("numDocs: " + indexSearcher.getIndexReader().numDocs());
		System.out.println("maxDocs: " + indexSearcher.getIndexReader().maxDoc());
		System.out.println("deleteDocs: " + indexSearcher.getIndexReader().numDeletedDocs());
		System.out.println();
		try {
			reMgr.release(indexSearcher);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void deleteDoc(String id) {
		try {
			targetGen = trackingIndexWriter.deleteDocuments(new Term("id", id));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void deleteAll() {
		try {
			targetGen = trackingIndexWriter.deleteAll();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 修改document，不需要关闭writer。实际是先删除再添加
	 * @param fields(id, oid, path, fileName, fileSuffix)
	 */
	public void update(Map<String, String> fields) {
		Document doc = new Document();
		try {
			doc.add(new StringField("id", fields.get("id"), Field.Store.YES));
			doc.add(new StringField("path", fields.get("path"), Field.Store.YES));
			doc.add(new StringField("fileName", fields.get("fileName"), Field.Store.YES));
			doc.add(new StringField("fileSuffix", fields.get("fileSuffix"), Field.Store.YES));
			doc.add(new TextField("content", new FileReader(new File(fields.get("path")))));
			targetGen = trackingIndexWriter.updateDocument(new Term("id", fields.get("oid")), doc);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void update(File file) throws Exception {
		validateExists(file);
		if (file.isDirectory()) {
			for (File subFile : file.listFiles()) {
				update(subFile);
			}
		} else if (file.isFile() && isTextType(file)) {
			// 创建Document
			Document doc = new Document();
			// 添加Field
			doc.add(new StringField("id", file.getName(), Field.Store.YES));
			doc.add(new StringField("path", file.getPath(), Field.Store.YES));
			doc.add(new StringField("fileName", file.getName().replace("." + getFileType(file), "").toLowerCase(), Field.Store.YES));
			doc.add(new StringField("fileSuffix", getFileType(file), Field.Store.YES));
			doc.add(new TextField("content", new FileReader(file)));
			// 更新文档到索引
			try {
				targetGen = trackingIndexWriter.updateDocument(new Term("id", file.getName()), doc);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * 查询完成后，将IndexSearcher释放回SearchFactory。通过reMgr.release(indexSearcher)
	 * @param queryString
	 * @param num
	 * @return
	 */
	public Map<Document, Float> doSearch(String queryString, int num) {
		Map<Document, Float> results = new LinkedHashMap<Document, Float>();
		try {
			crt.waitForGeneration(targetGen);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		IndexSearcher indexSearcher = getSearcher();
		TermQuery query = new TermQuery(new Term("content", queryString));
		try {
			TopDocs topDocs = indexSearcher.search(query, num);
			for(ScoreDoc sd : topDocs.scoreDocs) {
                Document doc = indexSearcher.doc(sd.doc);
				results.put(doc, sd.score);
            }
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				reMgr.release(indexSearcher);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return results;
	}
	
	public Map<Document, Float> searchByQuery(Query query, int num) {
		Map<Document, Float> results = new LinkedHashMap<Document, Float>();
		try {
			crt.waitForGeneration(targetGen);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		IndexSearcher indexSearcher = getSearcher();
		try {
			if (query == null) throw new Exception("Query is null");
			TopDocs tds = indexSearcher.search(query, num);
			for (ScoreDoc sd : tds.scoreDocs) {
				Document doc = indexSearcher.doc(sd.doc);
				results.put(doc, sd.score);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				reMgr.release(indexSearcher);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return results;
	}
	
    /***
     * 在使用时，searchAfter查询的是指定页数后面的数据，效率更高，推荐使用
     * @param query
     * @param pageIndex
     * @param pageSize
     * @return 
     */
    public Map<Document, Float> searchPageByAfter(String query, int pageIndex, int pageSize) {
		Map<Document, Float> results = new LinkedHashMap<Document, Float>();
		try {
			crt.waitForGeneration(targetGen);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		IndexSearcher indexSearcher = getSearcher();
        try {
            QueryParser parser = new QueryParser("content",new StandardAnalyzer());
            Query q =  parser.parse(query);
            //先获取上一页的最后一个元素
            ScoreDoc lastSd = getLastScoreDoc(pageIndex, pageSize, q, indexSearcher);
            //通过最后一个元素搜索下页的pageSize个元素
            TopDocs tds = indexSearcher.searchAfter(lastSd, q, pageSize);
            for(ScoreDoc sd : tds.scoreDocs) {
                Document doc = indexSearcher.doc(sd.doc);
				results.put(doc, sd.score);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
        	try {
				reMgr.release(indexSearcher);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return results;
    }
    
    public Map<Document, Float> searchPageByMultiField(Map<Occur, Map<String, String>> queries, int pageIndex, int pageSize) {
    	Map<Document, Float> results = new LinkedHashMap<Document, Float>();
    	try {
			crt.waitForGeneration(targetGen);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		IndexSearcher indexSearcher = getSearcher();
        try {
        	Builder occurBuilder = new BooleanQuery.Builder();
        	Builder shouldOccurBuilder = null;
        	if (queries.containsKey(BooleanClause.Occur.MUST_NOT)) {
        		Map<String, String> fields = queries.get(BooleanClause.Occur.MUST_NOT);
        		for (String field : fields.keySet()) {
					String[] keywords = fields.get(field).trim().split(" ");
					for (String keyword : keywords) {
						if (keyword.equals(" ")) continue;
						WildcardQuery query = new WildcardQuery(new Term(field, field.equals("content")?keyword:keyword.toLowerCase()));
						occurBuilder.add(query, BooleanClause.Occur.MUST_NOT);
					}
				}
        	}
        	
        	if (queries.containsKey(BooleanClause.Occur.SHOULD)) {
        		shouldOccurBuilder = new BooleanQuery.Builder();
        		Map<String, String> fields = queries.get(BooleanClause.Occur.SHOULD);
        		for (String field : fields.keySet()) {
					String[] keywords = fields.get(field).trim().split(" ");
					for (String keyword : keywords) {
						if (keyword.equals(" ")) continue;
						WildcardQuery query = new WildcardQuery(new Term(field, field.equals("content")?keyword:keyword.toLowerCase()));
						shouldOccurBuilder.add(query, BooleanClause.Occur.SHOULD);
					}
				}
        	}
        	
        	if (queries.containsKey(BooleanClause.Occur.MUST)) {
        		Map<String, String> fields = queries.get(BooleanClause.Occur.MUST);
        		for (String field : fields.keySet()) {
					String[] keywords = fields.get(field).trim().split(" ");
					for (String keyword : keywords) {
						if (keyword.equals(" ")) continue;
						WildcardQuery query = new WildcardQuery(new Term(field, field.equals("content")?keyword:keyword.toLowerCase()));
						occurBuilder.add(query, BooleanClause.Occur.MUST);
					}
				}
        	}
        	
    		if (shouldOccurBuilder != null) {
	    		BooleanQuery shouldOccurQuery = shouldOccurBuilder.build();
	    		occurBuilder.add(shouldOccurQuery, BooleanClause.Occur.MUST);
    		}
    		BooleanQuery query = occurBuilder.build();
    		System.out.println(query);
            //先获取上一页的最后一个元素
            ScoreDoc lastSd = getLastScoreDoc(pageIndex, pageSize, query, indexSearcher);
            //通过最后一个元素搜索下页的pageSize个元素
            TopDocs tds = indexSearcher.searchAfter(lastSd, query, pageSize);
            for(ScoreDoc sd : tds.scoreDocs) {
                Document doc = indexSearcher.doc(sd.doc);
				results.put(doc, sd.score);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
        	try {
				reMgr.release(indexSearcher);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
        System.out.println(results);
		return results;
    }
    
    /**
     * 根据页码和分页大小获取上一次的最后一个ScoreDoc
     */
    private ScoreDoc getLastScoreDoc(int pageIndex, int pageSize, Query query, IndexSearcher searcher) throws IOException {
        if (pageIndex == 1) return null;	//如果是第一页就返回空
        int num = pageSize * (pageIndex - 1);	//获取上一页的数量
        TopDocs tds = searcher.search(query, num);
        if (tds.totalHits == 0) return null;
        if (tds.totalHits < num) return tds.scoreDocs[tds.totalHits - 1];
        return tds.scoreDocs[num - 1];
    }
	
	private void addDocument(File file) throws Exception {
		validateExists(file);
		if (file.isDirectory()) {
			for (File subFile : file.listFiles()) {
				addDocument(subFile);
			}
		} else if (file.isFile() && isTextType(file)) {
			// 创建Document
			Document doc = new Document();
			// 添加Field
//			doc.add(new StringField("id", file.getName(), Field.Store.YES));
			doc.add(new StringField("path", file.getPath(), Field.Store.YES));
			doc.add(new StringField("fileName", file.getName().replace("." + getFileType(file), "").toLowerCase(), Field.Store.YES));
			doc.add(new StringField("fileSuffix", getFileType(file), Field.Store.YES));
			doc.add(new TextField("content", new FileReader(file)));
			// 添加文档到索引
			try {
				targetGen = trackingIndexWriter.addDocument(doc);
				if (fileNum % 10 == 0) System.out.println();
				System.out.print(fileNum++ + "/" + fileCount + "	");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	private void validateExists(File file) throws Exception {
		if (!file.exists()) {
			throw new Exception("file does not exists.");
		}
	}
	
	private boolean isTextType(File file) throws Exception {
		validateExists(file);
		return getFileType(file).equals("html") || getFileType(file).equals("js") || getFileType(file).equals("css");
	}
	
	private String getFileType(File file) {
		String fileName = file.getName();
		String fileType = fileName.substring(fileName.lastIndexOf(".") + 1, fileName.length());
		return fileType;
	}
	
	private IndexSearcher getSearcher() {
		IndexSearcher indexSearcher = null;
		try {
			reMgr.maybeRefresh(); // 刷新reMgr, 获取最新的IndexSearcher
			indexSearcher = reMgr.acquire();
			return indexSearcher;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return indexSearcher;
	}
	
	private void getFileCount(File file) throws Exception {
		validateExists(file);
		if (file.isDirectory()) {
			for (File f : file.listFiles()) {
				getFileCount(f);
			}
		} else if (file.isFile() && isTextType(file)) {
			fileCount++;
		}
	}
	
	@Override
	protected void finalize() throws Throwable {
		// stop the re-open thread
		crt.interrupt();
		crt.close();
		indexWriter.commit();
		indexWriter.flush();
		indexWriter.close();
		reMgr.close();
		super.finalize();
	}
}
