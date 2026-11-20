/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.zxyy.qagent.rag.vector;

import cn.hutool.core.util.StrUtil;
import com.zxyy.qagent.rag.config.RAGDefaultProperties;
import com.zxyy.qagent.framework.conversion.RetrievedChunk;
import com.zxyy.qagent.inf.embedding.EmbeddingService;
import com.zxyy.qagent.ingest.VectorTarget;
import com.zxyy.qagent.exception.ServiceException;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.BaseVector;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.response.SearchResp;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Slf4j
@Service
@RequiredArgsConstructor
@ConditionalOnProperty(name = "rag.vector.type", havingValue = "milvus", matchIfMissing = true)
public class MilvusVectorRetrieverService {

    private final EmbeddingService embeddingService;
    private final MilvusClientV2 milvusClient;
    private final RAGDefaultProperties ragDefaultProperties;

    public List<RetrievedChunk> retrieve(String query, VectorTarget target, int topK) {
        float[] vector = toArray(embeddingService.embed(query, target.embeddingModel()));
        if (vector.length != target.dimension()) {
            throw new ServiceException("查询向量维度与知识库配置不一致");
        }
        String filter = "collection_name == \"" + escapeFilterValue(target.partition()) + "\"";
        return searchShared(normalize(vector), filter, topK);
    }

    private String escapeFilterValue(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    /**
     * 在共享 collection 内执行一次向量检索
     *
     * @param filter 可选的标量过滤表达式（为空则不过滤，检索全共享库）
     */
    private List<RetrievedChunk> searchShared(float[] vector, String filter, int topK) {
        List<BaseVector> vectors = List.of(new FloatVec(vector));

        Map<String, Object> params = new HashMap<>();
        params.put("metric_type", ragDefaultProperties.getMetricType());
        params.put("ef", 128);

        var builder = SearchReq.builder()
                .collectionName(ragDefaultProperties.getCollectionName())
                .annsField("embedding")
                .data(vectors)
                .topK(topK)
                .searchParams(params)
                .outputFields(List.of("id", "content", "collection_name"));
        if (StrUtil.isNotBlank(filter)) {
            builder.filter(filter);
        }

        SearchResp resp = milvusClient.search(builder.build());
        List<List<SearchResp.SearchResult>> results = resp.getSearchResults();

        if (results == null || results.isEmpty()) {
            return List.of();
        }

        return results.get(0).stream()
                .map(r -> RetrievedChunk.builder()
                        .id(Objects.toString(r.getEntity().get("id"), ""))
                        .text(Objects.toString(r.getEntity().get("content"), ""))
                        .collectionName(Objects.toString(r.getEntity().get("collection_name"), null))
                        .score(r.getScore())
                        .build())
                .toList();
    }

    private static float[] toArray(List<Float> list) {
        float[] arr = new float[list.size()];
        for (int i = 0; i < list.size(); i++) arr[i] = list.get(i);
        return arr;
    }

    private static float[] normalize(float[] v) {
        double sum = 0.0;
        for (float x : v) sum += x * x;
        double len = Math.sqrt(sum);
        if (!Double.isFinite(len) || len == 0) {
            throw new ServiceException("查询向量为空或包含无效数值");
        }
        float[] nv = new float[v.length];
        for (int i = 0; i < v.length; i++) nv[i] = (float) (v[i] / len);
        return nv;
    }
}
