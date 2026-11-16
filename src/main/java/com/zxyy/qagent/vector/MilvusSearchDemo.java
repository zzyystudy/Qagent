package com.zxyy.qagent.vector;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.BaseVector;
import io.milvus.v2.service.vector.response.SearchResp;
import io.milvus.v2.service.vector.request.data.FloatVec;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import io.milvus.v2.service.vector.request.DeleteReq;

import static com.zxyy.qagent.vector.MilvusInsertDemo.getEmbeddings;


public class MilvusSearchDemo {
    public static void main(String[] args) throws IOException {
        // 1. 连接 Milvus
        ConnectConfig connectConfig = ConnectConfig.builder()
                .uri("http://192.168.249.192:19530")
                .build();
        MilvusClientV2 client = new MilvusClientV2(connectConfig);
        // 用户的问题
        String query = "买了东西不想要了怎么退货？";

        // 把问题向量化（复用前面的 getEmbeddings 方法）
        List<List<Float>> queryVectors = getEmbeddings(List.of(query));

        List<BaseVector> milvusQueryVectors = queryVectors.stream()
                .map(FloatVec::new)   // FloatVec(List<Float>)
                .collect(java.util.stream.Collectors.toList());

        // 执行向量检索
        SearchReq searchReq = SearchReq.builder()
                .collectionName("customer_service_chunks")
                .data(milvusQueryVectors)           // 查询向量
                .topK(3)                      // 返回最相似的 3 个结果
                .outputFields(List.of("chunk_text", "doc_id", "category"))  // 需要返回的字段
                .annsField("vector")          // 指定在哪个向量字段上检索
                .searchParams(Map.of("ef", 128))  // HNSW 检索时的搜索宽度
                .build();

        // 执行向量检索
        // 混合检索：向量相似度 + 标量过滤
        // 只在退货政策类的 chunk 里检索
        /*
        SearchReq filteredSearchReq = SearchReq.builder()
                .collectionName("customer_service_chunks")
                .data(milvusQueryVectors)
                .topK(3)
                .outputFields(List.of("chunk_text", "doc_id", "category"))
                .annsField("vector")
                .filter("category == \"return_policy\"")  // 只搜索退货政策类
                .searchParams(Map.of("ef", 128))
                .build();
         */

        SearchResp searchResp = client.search(searchReq);

        // 输出检索结果
        List<List<SearchResp.SearchResult>> results = searchResp.getSearchResults();
        for (List<SearchResp.SearchResult> resultList : results) {
            System.out.println("=== 检索结果 ===");
            for (int i = 0; i < resultList.size(); i++) {
                SearchResp.SearchResult result = resultList.get(i);
                System.out.println("Top-" + (i + 1) + "：");
                System.out.println("  相似度分数：" + result.getScore());
                System.out.println("  分类：" + result.getEntity().get("category"));
                System.out.println("  文档ID：" + result.getEntity().get("doc_id"));
                System.out.println("  内容：" + result.getEntity().get("chunk_text"));
                System.out.println();
            }
        }
    }

    public void delete(){


        // 1. 删除旧数据（通过 doc_id 定位）
        DeleteReq deleteReq = DeleteReq.builder()
                .collectionName("customer_service_chunks")
                .filter("doc_id == \"doc_return_001\"")
                .build();
        //client.delete(deleteReq);

        // 2. 对更新后的文档重新分块、向量化

        // 3. 插入新数据（同前面的插入流程）
    }
}
