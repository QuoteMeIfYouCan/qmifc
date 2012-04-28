package ca.openparliament.qmiyc.models;

import org.apache.hadoop.io.Text;

// COPY hansards_statement (id, document_id, "time", h1, h2, member_id, who, content_en, sequence, wordcount, politician_id, procedural, h3, who_hocid, content_fr, statement_type, written_question, source_id, who_context, slug, urlcache) FROM stdin;

public class OpenParliamentRecord {

	protected String id;
	protected String documentId;
	protected String time;
	protected String h1;
	protected String h2;
	protected String memberId;
	protected String who;
	protected String contentEn;
	protected String sequence;
	protected String wordcount;
	protected String politicianId;
	protected String procedural;
	protected String h3;
	protected String whoHocid;
	protected String contentFr;
	protected String statementType;
	protected String writtenQuestion;
	protected String sourceId;
	protected String whoContext;
	protected String slug;
	protected String urlcache;

	public OpenParliamentRecord(String inputString)
			throws IllegalArgumentException {
		String[] attributes = inputString.split("\t");
		int p = 0;
		id = attributes[p];
		p++;
		documentId = attributes[p];
		p++;
		time = attributes[p];
		p++;
		h1 = attributes[p];
		p++;
		h2 = attributes[p];
		p++;
		memberId = attributes[p];
		p++;
		who = attributes[p];
		p++;
		contentEn = attributes[p].replaceAll("\\<[^>]+\\>", "");
		p++;
		sequence = attributes[p];
		p++;
		wordcount = attributes[p];
		p++;
		politicianId = attributes[p];
		p++;
		procedural = attributes[p];
		p++;
		h3 = attributes[p];
		p++;
		whoHocid = attributes[p];
		p++;
		contentFr = attributes[p];
		p++;
		statementType = attributes[p];
		p++;
		writtenQuestion = attributes[p];
		p++;
		sourceId = attributes[p];
		p++;
		whoContext = attributes[p];
		p++;
		slug = attributes[p];
		p++;
		urlcache = attributes[p];
		p++;
	}

	public OpenParliamentRecord(Text inputText) throws IllegalArgumentException {
		this(inputText.toString());
	}

	public String getId() {
		return id;
	}

	public String getDocumentId() {
		return documentId;
	}

	public String getTime() {
		return time;
	}

	public String getH1() {
		return h1;
	}

	public String getH2() {
		return h2;
	}

	public String getMemberId() {
		return memberId;
	}

	public String getWho() {
		return who;
	}

	public String getContentEn() {
		return contentEn;
	}

	public String getSequence() {
		return sequence;
	}

	public String getWordcount() {
		return wordcount;
	}

	public String getPoliticianId() {
		return politicianId;
	}

	public String getProcedural() {
		return procedural;
	}

	public String getH3() {
		return h3;
	}

	public String getWhoHocid() {
		return whoHocid;
	}

	public String getContentFr() {
		return contentFr;
	}

	public String getStatementType() {
		return statementType;
	}

	public String getWrittenQuestion() {
		return writtenQuestion;
	}

	public String getSourceId() {
		return sourceId;
	}

	public String getWhoContext() {
		return whoContext;
	}

	public String getSlug() {
		return slug;
	}

	public String getUrlcache() {
		return urlcache;
	}

}
