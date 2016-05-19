package com.lucene;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.lucene.Main;
import com.lucene.config.LucenePropertiesConfigure;
import com.lucene.service.LuceneNRTServiceImpl;
import com.lucene.service.LuceneServiceImpl;

@ActiveProfiles("test")
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = Main.class)
public class LuceneTest {

	@Autowired
	private LucenePropertiesConfigure properties;

	@Autowired
	private LuceneServiceImpl luceneService;

	@Autowired
	private LuceneNRTServiceImpl luceneNRTService;

	private Map<Document, Float> docs;

	// @Test
	public void doIndex() {
		File indexDir = new File(properties.getIndexDir());
		for (File f : indexDir.listFiles()) {
			f.delete();
		}
		luceneService.doIndex(new File(properties.getDocDir()));
	}

	// @Test
	public void searchByTerm() {
		docs = luceneService.searchByTerm("fileName", "SortedDocValuesField", 100);
	}

	// @Test
	public void searchByTermRange() {
		docs = luceneService.searchByTermRange("fileName", "We", "Wf", 100);
	}

	// @Test
	public void searchByQueryParse() {
		try {
			QueryParser parser = new QueryParser("fileSuffix", new StandardAnalyzer());
			Query query = null;
			query = parser.parse("html");
			docs = luceneService.searchByQueryParse(query, 100);
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	// @Test
	public void ignoreScoreSearch() {
		try {
			QueryParser parser = new QueryParser("fileSuffix", new StandardAnalyzer());
			Query query = null;
			query = parser.parse("css js");
			ConstantScoreQuery constantScoreQuery = new ConstantScoreQuery(query);
			docs = luceneService.searchByQueryParse(constantScoreQuery, 100);
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	// @Test
	public void searchMultiByField() {
		List<List<String>> fieldsQuery = new ArrayList<List<String>>();
		List<String> fieldQueries = new ArrayList<String>();
		fieldQueries.add("content");
		fieldQueries.add("world");
		fieldsQuery.add(fieldQueries);
		fieldQueries = new ArrayList<String>();
		fieldQueries.add("fileName");
		fieldQueries.add("prettify");
		fieldsQuery.add(fieldQueries);
		docs = luceneService.searchMultiByField(fieldsQuery, 100);
	}

	// @Test
	public void searchMultiByTerms() {
		List<List<String>> fieldsQuery = new ArrayList<List<String>>();
		List<String> fieldQueries = new ArrayList<String>();
		fieldQueries.add("content");
		fieldQueries.add("world");
		fieldsQuery.add(fieldQueries);
		fieldQueries = new ArrayList<String>();
		fieldQueries.add("fileName");
		fieldQueries.add("prettify");
		fieldsQuery.add(fieldQueries);
		docs = luceneService.searchMultiByTerms(fieldsQuery, 100);
	}

	// @Test
	public void searchPageByAfter1() {
		docs = luceneService.searchPageByAfter("lucene", 1, 20);
	}

	// @Test
	public void searchPageByAfter2() {
		docs = luceneService.searchPageByAfter("lucene", 2, 20);
	}

	 @Test
	public void doIndexNRT() {
//		luceneNRTService.deleteAll();
		try {
			luceneNRTService.doIndex(new File(properties.getDocDir()));
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void doIndexAndUpdate() throws Exception {
//		luceneNRTService.deleteAll();
		luceneNRTService.getFileCount(new File(properties.getDocDir()));
		luceneNRTService.update(new File(properties.getDocDir()));
	}

	@Test
	public void searchPageByMultiField() {
		Map<BooleanClause.Occur, Map<String, String>> queries = new HashMap<BooleanClause.Occur, Map<String, String>>();
		Map<String, String> fields = new HashMap<String, String>();
		/*
		 * fields.put("content", "color"); queries.put(BooleanClause.Occur.MUST,
		 * fields);
		 * 
		 * fields = new HashMap<String, String>(); fields.put("content",
		 * "performan?? dimen?ion di*ent");
		 * queries.put(BooleanClause.Occur.MUST_NOT, fields);
		 * 
		 * fields = new HashMap<String, String>();
		 */
		fields.put("fileName", "Analyzer Plane closeIndexTask");
		fields.put("fileSuffix", "html js");
		fields.put("name", "industry");
		queries.put(BooleanClause.Occur.SHOULD, fields);

		try {
			docs = luceneNRTService.searchPageByMultiField(queries, 1, 10);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	@Before
	public void query() {
		luceneNRTService.query();
		System.out.println();
	}

	@After
	public void printDocs() {
		if (docs == null)
			return;
		System.out.println();
		for (Document document : docs.keySet()) {
			System.out.println("score: " + docs.get(document) + "	path: " + document.get("path"));
		}
		System.out.println("The number of results: " + docs.size() + "\n");
	}

}
