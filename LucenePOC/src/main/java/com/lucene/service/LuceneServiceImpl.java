package com.lucene.service;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BooleanQuery.Builder;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.lucene.config.LucenePropertiesConfigure;

@Component
public class LuceneServiceImpl {
	@Autowired
	private LucenePropertiesConfigure properties;
	private DirectoryReader reader;
	private int fileCount = 0;
	private int fileNum = 1;

	public void doIndex(File file) {
		IndexWriter writer = null;
		try {
			writer = getWriter();
			getFileCount(file);
			// 添加Document
			addDocument(file, writer);
			System.out.println("\nIndex finished.");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (writer != null)
					writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public Map<Document, Float> doSearch(String queryString, int n) {
		System.out.println("Query: " + queryString);
		QueryParser parser = new QueryParser("content", new StandardAnalyzer());
		Query query = null;
		try {
			query = parser.parse(queryString);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return searchByQueryParse(query, n);
	}

	/**
	 * 指定field进行查询，termquery不能进行数字和日期的查询 日期的查询需要转成数字进行查询，
	 * 数字查询使用NumbericRangeQuery
	 * 
	 * @param field
	 * @param queryString
	 * @param num
	 * @return
	 * @throws Exception
	 */
	public Map<Document, Float> searchByTerm(String field, String queryString, int num) {
		System.out.println("searchByTerm Key words: " + queryString + " Field: " + field);
		Map<Document, Float> results = new LinkedHashMap<Document, Float>();
		try {
			IndexSearcher searcher = getSearcher();
			Query query = new TermQuery(new Term(field, queryString));
			TopDocs tds = searcher.search(query, num);
			for (ScoreDoc sd : tds.scoreDocs) {
				Document doc = searcher.doc(sd.doc);
				results.put(doc, sd.score);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return results;
	}
	
	/**
	 * 范围查询
	 * 
	 * @param field
	 * @param start
	 * @param end
	 * @param num
	 * @return
	 */
	public Map<Document, Float> searchByTermRange(String field, String start, String end, int num) {
		System.out.println("searchByTermRange Start: " + start + " End: " + end + " Field: " + field);
		Map<Document, Float> results = new LinkedHashMap<Document, Float>();
		try {
			IndexSearcher searcher = getSearcher();
			Query query = new TermRangeQuery(field, new BytesRef(start.getBytes()), new BytesRef(end.getBytes()), true, true);
			TopDocs tds = searcher.search(query, num);
			for (ScoreDoc sd : tds.scoreDocs) {
				Document doc = searcher.doc(sd.doc);
				results.put(doc, sd.score);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return results;
	}
	
	/**
	 * 所字段搜索。
	 * BooleanClause.Occur.MUST = and, BooleanClause.Occur.MUST_NOT = not, BooleanClause.Occur.SHOULD = or, BooleanClause.Occur.FILTER = and (不计算分值)
	 * @param fieldsQuery
	 * @param num
	 * @return
	 */
	public Map<Document, Float> searchMultiByField(List<List<String>> fieldsQuery, int num) {
		System.out.println("searchMultiByField Search fields: " + fieldsQuery);
		List<String> queries = new ArrayList<String>();
		List<String> fields = new ArrayList<String>();
		List<BooleanClause.Occur> flags = new ArrayList<BooleanClause.Occur>();
		for (List<String> fieldQueries : fieldsQuery) {
			fields.add(fieldQueries.get(0));
			queries.add(fieldQueries.get(1));
			if (fieldQueries.get(0).equals("fileSuffix")) {
				flags.add(BooleanClause.Occur.MUST);
			} else {
				flags.add(BooleanClause.Occur.SHOULD);
			}
		}
		Query query = null;
		try {
			query = MultiFieldQueryParser.parse(queries.toArray(new String[queries.size()]), fields.toArray(new String[fields.size()]), flags.toArray(new BooleanClause.Occur[flags.size()]), new StandardAnalyzer());
		} catch (Exception e) {
			e.printStackTrace();
		}
		return searchByQueryParse(query, num);
	}
	
	/**
	 * 多词搜索
	 * @param fieldsQuery
	 * @param num
	 * @return
	 */
	public Map<Document, Float> searchMultiByTerms(List<List<String>> fieldsQuery, int num) {
		System.out.println("searchMultiByTerms Fields query: " + fieldsQuery);
		Builder builder = new BooleanQuery.Builder();
		for (List<String> fieldQueries : fieldsQuery) {
			TermQuery termQuery = new TermQuery(new Term(fieldQueries.get(0), fieldQueries.get(1)));
			if (fieldQueries.get(0).equals("fileSuffix")) {
				builder.add(termQuery, BooleanClause.Occur.MUST);
			} else {
				builder.add(termQuery, BooleanClause.Occur.SHOULD);
			}
		}
		BooleanQuery query = builder.build();
		return searchByQueryParse(query, num);
	}

	/**
	 * 基于query查询
	 * 
	 * @param query
	 * @param num
	 * @return
	 */
	public Map<Document, Float> searchByQueryParse(Query query, int num) {
		System.out.println("searchByQueryParse Query: " + query);
		Map<Document, Float> results = new LinkedHashMap<Document, Float>();
		try {
			if (query == null) throw new Exception("Query is null");
			IndexSearcher searcher = getSearcher();
			TopDocs tds = searcher.search(query, num);
			for (ScoreDoc sd : tds.scoreDocs) {
				Document doc = searcher.doc(sd.doc);
				results.put(doc, sd.score);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return results;
	}

	/**
	 * 分页查询
	 * 
	 * @param query
	 * @param pageIndex
	 * @param pageSize
	 * @return
	 */
	public Map<Document, Float> searchPageByQueryParse(Query query, int pageIndex, int pageSize) {
		System.out.println("searchPageByQueryParse Query: " + query);
		Map<Document, Float> results = new LinkedHashMap<Document, Float>();
		try {
			IndexSearcher searcher = getSearcher();
			TopDocs tds = searcher.search(query, pageSize * pageIndex);
			ScoreDoc[] sds = tds.scoreDocs;
			int start = (pageIndex - 1) * pageSize;
			int end = pageIndex * pageSize;
			if (end >= sds.length)
				end = sds.length;
			for (int i = start; i < end; i++) {
				Document doc = searcher.doc(sds[i].doc);
				results.put(doc, sds[i].score);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return results;
	}
	
	public Map<Document, Float> search(Map<Occur, Map<String, String>> queries, int pageNum, int pageSize) throws Exception {
		Map<Document, Float> results = new LinkedHashMap<Document, Float>();
		IndexSearcher indexSearcher = getSearcher();
		Builder occurBuilder = new BooleanQuery.Builder();
		Builder shouldOccurBuilder = null;
		Builder mustOccurBuilder = null;
		if (queries.containsKey(Occur.MUST_NOT)) {
			Map<String, String> fields = queries.get(Occur.MUST_NOT);
			for (String field : fields.keySet()) {
				String[] keywords = fields.get(field).trim().split("\\s+");
				for (String keyword : keywords) {
					WildcardQuery query = new WildcardQuery(new Term(field, keyword.toLowerCase()));
					occurBuilder.add(query, Occur.MUST_NOT);
				}
			}
		}

		if (queries.containsKey(Occur.SHOULD)) {
			shouldOccurBuilder = new BooleanQuery.Builder();
			Map<String, String> shouldFields = queries.get(Occur.SHOULD);
			for (String shouldField : shouldFields.keySet()) {
				String[] shouldKeywords = shouldFields.get(shouldField).trim().split("\\s+");
				for (String shouldKeyword : shouldKeywords) {
					WildcardQuery shouldQuery = new WildcardQuery(new Term(shouldField, "*" + shouldKeyword.toLowerCase() + "*"));
					shouldOccurBuilder.add(shouldQuery, Occur.SHOULD);
				}
			}
			// Each "MUST" condition is appended to a set of "SHOULD" conditions
			if (queries.containsKey(Occur.MUST) && queries.get(Occur.MUST).size() > 0) {
				Map<String, String> mustFields = queries.get(Occur.MUST);
				for (String mustField : mustFields.keySet()) {
					WildcardQuery mustQuery = new WildcardQuery(new Term(mustField, "*" + mustFields.get(mustField).trim().toLowerCase() + "*"));
					mustOccurBuilder = new BooleanQuery.Builder();
					mustOccurBuilder.add(mustQuery, Occur.MUST);
					BooleanQuery mustOccurQuery = mustOccurBuilder.build();
					occurBuilder.add(mustOccurQuery, Occur.SHOULD);
				}
			}
			BooleanQuery shouldOccurQuery = shouldOccurBuilder.build();
			occurBuilder.add(shouldOccurQuery, Occur.MUST);
		} else if (queries.containsKey(Occur.MUST) && queries.get(Occur.MUST).size() > 0) {
			Map<String, String> mustFields = queries.get(Occur.MUST);
			for (String mustField : mustFields.keySet()) {
				WildcardQuery mustQuery = new WildcardQuery(new Term(mustField, "*" + mustFields.get(mustField).trim().toLowerCase() + "*"));
				mustOccurBuilder = new BooleanQuery.Builder();
				mustOccurBuilder.add(mustQuery, Occur.MUST);
				BooleanQuery mustOccurQuery = mustOccurBuilder.build();
				occurBuilder.add(mustOccurQuery, Occur.SHOULD);
			}
		}

		BooleanQuery query = occurBuilder.build();
		TopDocs tds;
		if (pageSize != 0 && pageNum != 0) {
			// Get the last doc of the previous page
			ScoreDoc lastSd = getLastScoreDoc(pageNum, pageSize, query, indexSearcher);
			// Search the next page of the docs, through the last doc
			tds = indexSearcher.searchAfter(lastSd, query, pageSize);
		} else {
			// Search top 1000 docs if there are no pageSize and pageNum provided.
			tds = indexSearcher.search(query, 1000);
		}
		for (ScoreDoc sd : tds.scoreDocs) {
			Document doc = indexSearcher.doc(sd.doc);
			results.put(doc, sd.score);
		}
		return results;
	}
	
	/**
     * 根据页码和分页大小获取上一次的最后一个ScoreDoc
     */
    private ScoreDoc getLastScoreDoc(int pageIndex, int pageSize, Query query, IndexSearcher searcher) throws IOException {
        if (pageIndex == 1) return null;	//如果是第一页就返回空
        int num = pageSize * (pageIndex - 1);	//获取上一页的数量
        TopDocs tds = searcher.search(query, num);
        return tds.scoreDocs[num - 1];
    }
    
    /***
     * 在使用时，searchAfter查询的是指定页数后面的数据，效率更高，推荐使用
     * @param query
     * @param pageIndex
     * @param pageSize
     * @return 
     */
    public Map<Document, Float> searchPageByAfter(String query, int pageIndex, int pageSize) {
    	System.out.println("searchPageByAfter Query: " + query);
		Map<Document, Float> results = new LinkedHashMap<Document, Float>();
        try {
            IndexSearcher searcher = getSearcher();
            QueryParser parser = new QueryParser("content",new StandardAnalyzer());
            Query q =  parser.parse(query);
            //先获取上一页的最后一个元素
            ScoreDoc lastSd = getLastScoreDoc(pageIndex, pageSize, q, searcher);
            //通过最后一个元素搜索下页的pageSize个元素
            TopDocs tds = searcher.searchAfter(lastSd, q, pageSize);
            for(ScoreDoc sd : tds.scoreDocs) {
                Document doc = searcher.doc(sd.doc);
				results.put(doc, sd.score);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
		return results;
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

	private void addDocument(File file, IndexWriter writer) throws Exception {
		validateExists(file);
		if (file.isDirectory()) {
			for (File subFile : file.listFiles()) {
				addDocument(subFile, writer);
			}
		} else if (file.isFile() && isTextType(file)) {
			// 创建Document
			Document doc = new Document();
			System.out.print(fileNum + "/" + fileCount + " ");
			// 添加Field
			doc.add(new IntField("id", fileNum++, Field.Store.YES));
			doc.add(new StringField("path", file.getPath(), Field.Store.YES));
			doc.add(new StringField("fileName", file.getName().replace("." + getFileType(file), ""), Field.Store.YES));
			doc.add(new StringField("fileSuffix", getFileType(file), Field.Store.YES));
			doc.add(new TextField("content", new FileReader(file)));
			// 添加文档到索引
			writer.addDocument(doc);
		}
	}

	private IndexWriter getWriter() throws Exception {
		IndexWriter writer = null;
		// 创建Directory
		File indexDir = new File(properties.getIndexDir());
		validateExists(indexDir);
		Directory directory = FSDirectory.open(indexDir.toPath());
		// 创建IndexWriter
		IndexWriterConfig writerConfig = new IndexWriterConfig(new StandardAnalyzer());
		writer = new IndexWriter(directory, writerConfig);
		return writer;
	}

	private IndexSearcher getSearcher() throws Exception {
		if (reader == null) {
			// 创建Directory
			File indexDir = new File(properties.getIndexDir());
			validateExists(indexDir);
			Directory directory = FSDirectory.open(indexDir.toPath());
			// 创建Reader
			reader = DirectoryReader.open(directory);
		} else {
			// 实现近实时查询，不关闭reader，但是Index有变化时，重新获取reader
			DirectoryReader tr = DirectoryReader.openIfChanged(reader);
			if (tr != null) {
				reader.close();
				reader = tr;
			}
		}
		// 根据IndexReader创建IndexSearcher
		IndexSearcher searcher = new IndexSearcher(reader);
		return searcher;
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

	private void validateExists(File file) throws Exception {
		if (!file.exists()) {
			throw new Exception("file does not exists.");
		}
	}

	@Override
	protected void finalize() throws Throwable {
		if (reader != null)
			reader.close();
		super.finalize();
	}

}
