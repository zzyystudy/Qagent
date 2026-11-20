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

package com.zxyy.qagent.knowledge.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.collection.CollUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.zxyy.qagent.inf.model.ModelSelector;
import com.zxyy.qagent.rag.config.RAGDefaultProperties;
import com.zxyy.qagent.knowledge.controller.request.KnowledgeBaseCreateRequest;
import com.zxyy.qagent.knowledge.controller.request.KnowledgeBasePageRequest;
import com.zxyy.qagent.knowledge.controller.vo.KnowledgeBaseVO;
import com.zxyy.qagent.knowledge.dao.entity.KnowledgeBaseDO;
import com.zxyy.qagent.knowledge.dao.entity.KnowledgeDocumentDO;
import com.zxyy.qagent.knowledge.dao.mapper.KnowledgeBaseMapper;
import com.zxyy.qagent.knowledge.dao.mapper.KnowledgeDocumentMapper;
import com.zxyy.qagent.framework.context.UserContext;
import com.zxyy.qagent.exception.ClientException;
import com.zxyy.qagent.rag.vector.VectorSpaceId;
import com.zxyy.qagent.rag.vector.VectorSpaceSpec;
import com.zxyy.qagent.rag.vector.VectorStoreAdmin;
import com.zxyy.qagent.rag.service.FileStorageService;
import com.zxyy.qagent.knowledge.service.KnowledgeBaseService;
import lombok.RequiredArgsConstructor;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import java.util.HashMap;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class KnowledgeBaseServiceImpl implements KnowledgeBaseService {

    private final KnowledgeBaseMapper knowledgeBaseMapper;
    private final KnowledgeDocumentMapper knowledgeDocumentMapper;
    private final VectorStoreAdmin vectorStoreAdmin;
    private final FileStorageService fileStorageService;
    private final ModelSelector modelSelector;
    private final RAGDefaultProperties ragDefaultProperties;

    @Transactional
    @Override
    public String create(KnowledgeBaseCreateRequest requestParam) {
        String name = requestParam.getName() == null ? "" : requestParam.getName().strip();
        if (name.isEmpty() || name.length() > 50) {
            throw new ClientException("知识库名称须为 1 到 50 个字符");
        }
        String collectionName = requestParam.getCollectionName();
        if (collectionName == null || !collectionName.matches("[a-z0-9]{1,50}")) {
            throw new ClientException("Collection 名称须为 1 到 50 位小写字母或数字");
        }
        String modelId = requestParam.getEmbeddingModel();
        boolean modelAvailable = modelSelector.selectEmbeddingCandidates().stream()
                .anyMatch(target -> target.id().equals(modelId)
                        && Objects.equals(target.candidate().getDimension(), ragDefaultProperties.getDimension()));
        if (!modelAvailable) {
            throw new ClientException("请选择维度匹配且可用的 Embedding 模型");
        }

        // 名称重复校验
        Long count = knowledgeBaseMapper.selectCount(
                new LambdaQueryWrapper<KnowledgeBaseDO>()
                        .eq(KnowledgeBaseDO::getName, name)
                        .eq(KnowledgeBaseDO::getDeleted, 0)
        );
        if (count > 0) {
            throw new ClientException("知识库名称已存在：" + requestParam.getName());
        }

        // Collection 名重复校验（共享 collection 模型下，向量层不再拦截重复，需在此显式校验）
        Long collectionCount = knowledgeBaseMapper.selectCount(
                new LambdaQueryWrapper<KnowledgeBaseDO>()
                        .eq(KnowledgeBaseDO::getCollectionName, collectionName)
                        .eq(KnowledgeBaseDO::getDeleted, 0)
        );
        if (collectionCount > 0) {
            throw new ClientException("Collection 名称已存在：" + collectionName);
        }

        Date now = new Date();
        KnowledgeBaseDO kbDO = KnowledgeBaseDO.builder()
                .name(name)
                .embeddingModel(modelId)
                .collectionName(collectionName)
                .createdBy(UserContext.getUsername())
                .updatedBy(UserContext.getUsername())
                .createTime(now)
                .updateTime(now)
                .deleted(0)
                .build();

        try {
            knowledgeBaseMapper.insert(kbDO);
        } catch (DuplicateKeyException e) {
            throw new ClientException("Collection 名称已存在：" + collectionName);
        }

        // 在全局知识库桶下建立该知识库目录（幂等），collectionName 即目录名
        fileStorageService.createKnowledgeSpace(collectionName);

        VectorSpaceSpec spaceSpec = VectorSpaceSpec.builder()
                .spaceId(VectorSpaceId.builder()
                        .logicalName(collectionName)
                        .build())
                .remark(name)
                .build();
        vectorStoreAdmin.ensureVectorSpace(spaceSpec);

        return String.valueOf(kbDO.getId());
    }

    @Override
    public KnowledgeBaseVO queryById(String kbId) {
        KnowledgeBaseDO kbDO = knowledgeBaseMapper.selectById(kbId);
        if (kbDO == null || kbDO.getDeleted() != null && kbDO.getDeleted() == 1) {
            throw new ClientException("知识库不存在");
        }
        return BeanUtil.toBean(kbDO, KnowledgeBaseVO.class);
    }

    @Override
    public IPage<KnowledgeBaseVO> pageQuery(KnowledgeBasePageRequest requestParam) {
        LambdaQueryWrapper<KnowledgeBaseDO> queryWrapper = Wrappers.lambdaQuery(KnowledgeBaseDO.class)
                .like(StringUtils.hasText(requestParam.getName()), KnowledgeBaseDO::getName, requestParam.getName())
                .eq(KnowledgeBaseDO::getDeleted, 0)
                .orderByDesc(KnowledgeBaseDO::getUpdateTime)
                .orderByDesc(KnowledgeBaseDO::getId);

        Page<KnowledgeBaseDO> page = new Page<>(Math.max(1, requestParam.getCurrent()), Math.max(1, Math.min(100, requestParam.getSize())));
        IPage<KnowledgeBaseDO> result = knowledgeBaseMapper.selectPage(page, queryWrapper);
        Map<String, Long> docCountMap = new HashMap<>();
        if (CollUtil.isNotEmpty(result.getRecords())) {
            List<String> kbIds = result.getRecords().stream()
                    .map(KnowledgeBaseDO::getId)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
            if (!kbIds.isEmpty()) {
                List<Map<String, Object>> rows = knowledgeDocumentMapper.selectMaps(
                        Wrappers.query(KnowledgeDocumentDO.class)
                                .select("kb_id", "COUNT(1) AS doc_count")
                                .in("kb_id", kbIds)
                                .eq("deleted", 0)
                                .groupBy("kb_id")
                );
                for (Map<String, Object> row : rows) {
                    Object kbIdValue = row.get("kb_id");
                    Object countValue = row.get("doc_count");
                    if (kbIdValue == null || countValue == null) {
                        continue;
                    }
                    docCountMap.put(kbIdValue.toString(), ((Number) countValue).longValue());
                }
            }
        }
        return result.convert(each -> {
            KnowledgeBaseVO vo = BeanUtil.toBean(each, KnowledgeBaseVO.class);
            Long docCount = docCountMap.get(each.getId());
            vo.setDocumentCount(docCount != null ? docCount : 0L);
            return vo;
        });
    }
}
