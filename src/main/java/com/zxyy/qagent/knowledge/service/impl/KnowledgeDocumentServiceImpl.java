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
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.zxyy.qagent.errorcode.BaseErrorCode;
import com.zxyy.qagent.exception.ClientException;
import com.zxyy.qagent.exception.ServiceException;
import com.zxyy.qagent.framework.context.UserContext;
import com.zxyy.qagent.ingest.DocumentRef;
import com.zxyy.qagent.ingest.IngestionKernel;
import com.zxyy.qagent.ingest.IngestionOutcome;
import com.zxyy.qagent.ingest.IngestionSpec;
import com.zxyy.qagent.ingest.VectorTarget;
import com.zxyy.qagent.ingest.domain.enums.SourceType;
import com.zxyy.qagent.ingest.sink.ChunkIndexWriter;
import com.zxyy.qagent.knowledge.controller.request.KnowledgeDocumentPageRequest;
import com.zxyy.qagent.knowledge.controller.request.KnowledgeDocumentUpdateRequest;
import com.zxyy.qagent.knowledge.controller.request.KnowledgeDocumentUploadRequest;
import com.zxyy.qagent.knowledge.controller.vo.KnowledgeDocumentChunkLogVO;
import com.zxyy.qagent.knowledge.controller.vo.KnowledgeDocumentSearchVO;
import com.zxyy.qagent.knowledge.controller.vo.KnowledgeDocumentVO;
import com.zxyy.qagent.knowledge.dao.entity.KnowledgeBaseDO;
import com.zxyy.qagent.knowledge.dao.entity.KnowledgeChunkDO;
import com.zxyy.qagent.knowledge.dao.entity.KnowledgeDocumentChunkLogDO;
import com.zxyy.qagent.knowledge.dao.entity.KnowledgeDocumentDO;
import com.zxyy.qagent.knowledge.dao.mapper.KnowledgeBaseMapper;
import com.zxyy.qagent.knowledge.dao.mapper.KnowledgeChunkMapper;
import com.zxyy.qagent.knowledge.dao.mapper.KnowledgeDocumentChunkLogMapper;
import com.zxyy.qagent.knowledge.dao.mapper.KnowledgeDocumentMapper;
import com.zxyy.qagent.knowledge.enums.DocumentStatus;
import com.zxyy.qagent.knowledge.service.KnowledgeChunkService;
import com.zxyy.qagent.knowledge.service.KnowledgeDocumentService;
import com.zxyy.qagent.knowledge.support.IngestionSpecCodec;
import com.zxyy.qagent.knowledge.support.VectorTargetResolver;
import com.zxyy.qagent.rag.chunk.model.EmbeddedChunk;
import com.zxyy.qagent.rag.dto.StoredFileDTO;
import com.zxyy.qagent.rag.registry.ParserRegistry;
import com.zxyy.qagent.rag.service.FileStorageService;
import com.zxyy.qagent.rag.vector.VectorStoreService;
import com.zxyy.qagent.util.DisplayType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionOperations;
import org.springframework.util.StringUtils;
import org.springframework.web.multipart.MultipartFile;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class KnowledgeDocumentServiceImpl implements KnowledgeDocumentService {

    private static final String CHUNK_MODE = "chunk";

    private final KnowledgeBaseMapper knowledgeBaseMapper;
    private final KnowledgeDocumentMapper documentMapper;
    private final ParserRegistry parserRegistry;
    private final IngestionKernel ingestionKernel;
    private final ChunkIndexWriter chunkIndexWriter;
    private final IngestionSpecCodec ingestionSpecCodec;
    private final FileStorageService fileStorageService;
    private final VectorStoreService vectorStoreService;
    private final KnowledgeChunkService knowledgeChunkService;
    private final KnowledgeDocumentChunkLogMapper chunkLogMapper;
    private final KnowledgeChunkMapper chunkMapper;
    private final TransactionOperations transactionOperations;
    private final VectorTargetResolver vectorTargetResolver;

    @Override
    public KnowledgeDocumentVO upload(String kbId, KnowledgeDocumentUploadRequest request, MultipartFile file) {
        VectorTarget target = vectorTargetResolver.resolve(knowledgeBaseMapper.selectById(kbId));
        validateProcessingOptions(request.getProcessMode(), request.getPipelineId(),
                request.getSourceLocation(), Boolean.TRUE.equals(request.getScheduleEnabled()), request.getScheduleCron());
        String fileSource = SourceType.FILE.getValue();
        if (StringUtils.hasText(request.getSourceType()) && !fileSource.equalsIgnoreCase(request.getSourceType().trim())) {
            throw new ClientException("当前仅支持上传本地文件");
        }
        if (file == null || file.isEmpty()) {
            throw new ClientException("上传文件不能为空");
        }

        String spec = ingestionSpecCodec.normalize(request.getIngestionSpec());
        StoredFileDTO stored = fileStorageService.upload(target.partition(), file);
        var now = new Date();
        String operator = UserContext.getUsername();
        KnowledgeDocumentDO document = KnowledgeDocumentDO.builder()
                .kbId(kbId)
                .docName(stored.getOriginalFilename())
                .enabled(1)
                .chunkCount(0)
                .fileUrl(stored.getUrl())
                .fileType(stored.getDetectedType())
                .mimeType(stored.getMimeType())
                .fileSize(stored.getSize())
                .status(DocumentStatus.PENDING.getCode())
                .sourceType(fileSource)
                .scheduleEnabled(0)
                .processMode(CHUNK_MODE)
                .ingestionSpec(spec)
                .createdBy(operator)
                .updatedBy(operator)
                .createTime(now)
                .updateTime(now)
                .build();
        try {
            if (!parserRegistry.canParse(stored.getMimeType())) {
                throw new ClientException("暂不支持的文件类型：" + stored.getDetectedType());
            }
            documentMapper.insert(document);
        } catch (RuntimeException e) {
            deleteStoredFileQuietly(document);
            throw e;
        }
        return toVO(document);
    }

    @Override
    public void startChunk(String docId) {
        var startedAt = new Date();
        KnowledgeDocumentChunkLogDO chunkLog = KnowledgeDocumentChunkLogDO.builder()
                .docId(docId)
                .status(DocumentStatus.RUNNING.getCode())
                .processMode(CHUNK_MODE)
                .startTime(startedAt)
                .createTime(startedAt)
                .updateTime(startedAt)
                .build();

        KnowledgeDocumentDO document = transactionOperations.execute(transaction -> {
            KnowledgeDocumentDO current = requireEditableDocument(docId);
            if (!Integer.valueOf(1).equals(current.getEnabled())) {
                throw new ClientException("文档未启用，暂不支持分块操作");
            }
            requireChunkMode(current.getProcessMode());
            updateChunkStatus(docId, DocumentStatus.RUNNING, null);
            chunkLogMapper.insert(chunkLog);
            return current;
        });

        try {
            VectorTarget target = vectorTargetResolver.resolve(knowledgeBaseMapper.selectById(document.getKbId()));
            IngestionSpec spec = ingestionSpecCodec.read(document.getIngestionSpec());
            chunkLog.setParseProfile(spec.parseProfile().getCode());
            IngestionOutcome outcome = ingestionKernel.run(documentRef(document), readFileBytes(document), spec, target);
            IngestionOutcome.IngestionTimings timings = outcome.timings();
            chunkLog.setExtractDuration(timings.parseMillis());
            chunkLog.setChunkDuration(timings.chunkMillis());
            chunkLog.setEmbedDuration(timings.embedMillis());
            chunkLog.setPersistDuration(timings.indexMillis());
            chunkLog.setChunkCount(outcome.chunkCount());
            finishChunk(chunkLog, DocumentStatus.SUCCESS, outcome);
        } catch (Exception e) {
            log.error("文档分块失败，docId={}", docId, e);
            chunkLog.setErrorMessage(e.getMessage());
            chunkLog.setChunkCount(0);
            try {
                finishChunk(chunkLog, DocumentStatus.FAILED, null);
            } catch (RuntimeException statusError) {
                e.addSuppressed(statusError);
                log.error("保存文档分块失败状态时出错，docId={}", docId, statusError);
            }
            throw new ServiceException("文档分块失败，请查看分块日志", e, BaseErrorCode.SERVICE_ERROR);
        }
    }

    private void finishChunk(KnowledgeDocumentChunkLogDO chunkLog, DocumentStatus status, IngestionOutcome outcome) {
        Date finishedAt = new Date();
        chunkLog.setStatus(status.getCode());
        chunkLog.setEndTime(finishedAt);
        chunkLog.setUpdateTime(finishedAt);
        chunkLog.setTotalDuration(finishedAt.getTime() - chunkLog.getStartTime().getTime());
        transactionOperations.executeWithoutResult(transaction -> {
            chunkLogMapper.updateById(chunkLog);
            updateChunkStatus(chunkLog.getDocId(), status, outcome);
        });
    }

    private void updateChunkStatus(String docId, DocumentStatus status, IngestionOutcome outcome) {
        LambdaUpdateWrapper<KnowledgeDocumentDO> update = Wrappers.lambdaUpdate(KnowledgeDocumentDO.class)
                .eq(KnowledgeDocumentDO::getId, docId)
                .set(KnowledgeDocumentDO::getStatus, status.getCode())
                .set(KnowledgeDocumentDO::getUpdatedBy, UserContext.getUsername())
                .set(KnowledgeDocumentDO::getUpdateTime, new Date());
        if (outcome != null) {
            update.set(KnowledgeDocumentDO::getChunkCount, outcome.chunkCount())
                    .set(StringUtils.hasText(outcome.mimeType()), KnowledgeDocumentDO::getMimeType, outcome.mimeType());
        }
        documentMapper.update(update);
    }

    private byte[] readFileBytes(KnowledgeDocumentDO document) {
        try (InputStream in = fileStorageService.openStream(document.getFileUrl())) {
            return in.readAllBytes();
        } catch (Exception e) {
            throw new ServiceException("读取文档源文件失败", e, BaseErrorCode.SERVICE_ERROR);
        }
    }

    private DocumentRef documentRef(KnowledgeDocumentDO document) {
        return new DocumentRef(document.getId(), document.getKbId(), document.getDocName());
    }

    @Override
    public void delete(String docId) {
        KnowledgeDocumentDO document = transactionOperations.execute(transaction -> {
            KnowledgeDocumentDO current = requireEditableDocument(docId);
            VectorTarget target = vectorTargetResolver.resolve(knowledgeBaseMapper.selectById(current.getKbId()));
            chunkIndexWriter.deleteDocument(target, documentRef(current));
            chunkLogMapper.delete(Wrappers.lambdaQuery(KnowledgeDocumentChunkLogDO.class)
                    .eq(KnowledgeDocumentChunkLogDO::getDocId, docId));
            documentMapper.deleteById(current);
            return current;
        });
        deleteStoredFileQuietly(document);
    }

    @Override
    public KnowledgeDocumentVO get(String docId) {
        return toVO(requireDocument(documentMapper.selectById(docId)));
    }

    @Override
    @Transactional
    public void update(String docId, KnowledgeDocumentUpdateRequest request) {
        requireEditableDocument(docId);
        validateProcessingOptions(request.getProcessMode(), request.getPipelineId(), request.getSourceLocation(),
                Integer.valueOf(1).equals(request.getScheduleEnabled()), request.getScheduleCron());
        if (!StringUtils.hasText(request.getDocName())) {
            throw new ClientException("文档名称不能为空");
        }

        LambdaUpdateWrapper<KnowledgeDocumentDO> update = Wrappers.lambdaUpdate(KnowledgeDocumentDO.class)
                .eq(KnowledgeDocumentDO::getId, docId)
                .set(KnowledgeDocumentDO::getDocName, request.getDocName().trim())
                .set(KnowledgeDocumentDO::getUpdatedBy, UserContext.getUsername())
                .set(KnowledgeDocumentDO::getUpdateTime, new Date());
        if (request.getIngestionSpec() != null || StringUtils.hasText(request.getProcessMode())) {
            update.set(KnowledgeDocumentDO::getIngestionSpec, ingestionSpecCodec.normalize(request.getIngestionSpec()))
                    .set(KnowledgeDocumentDO::getProcessMode, CHUNK_MODE)
                    .set(KnowledgeDocumentDO::getPipelineId, null);
        }
        documentMapper.update(update);
    }

    private void validateProcessingOptions(String mode, String pipelineId, String sourceLocation,
                                           boolean scheduleEnabled, String scheduleCron) {
        requireChunkMode(mode);
        if (StringUtils.hasText(pipelineId)) {
            throw new ClientException("当前仅支持直接分块（processMode=chunk）");
        }
        if (StringUtils.hasText(sourceLocation) || scheduleEnabled || StringUtils.hasText(scheduleCron)) {
            throw new ClientException("当前仅支持上传本地文件");
        }
    }

    private void requireChunkMode(String mode) {
        if (StringUtils.hasText(mode) && !CHUNK_MODE.equalsIgnoreCase(mode.trim())) {
            throw new ClientException("当前仅支持直接分块（processMode=chunk）");
        }
    }

    private KnowledgeDocumentDO requireDocument(KnowledgeDocumentDO document) {
        if (document == null) {
            throw new ClientException("文档不存在");
        }
        return document;
    }

    // 调用方在事务内锁定文档行，避免领取分块任务时与文档变更交错。
    private KnowledgeDocumentDO requireEditableDocument(String docId) {
        KnowledgeDocumentDO document = requireDocument(documentMapper.selectOne(
                Wrappers.lambdaQuery(KnowledgeDocumentDO.class)
                        .eq(KnowledgeDocumentDO::getId, docId)
                        .last("FOR UPDATE")));
        if (DocumentStatus.RUNNING.getCode().equals(document.getStatus())) {
            throw new ClientException("文档正在分块，请稍后再试");
        }
        return document;
    }


    @Override
    public IPage<KnowledgeDocumentVO> page(String kbId, KnowledgeDocumentPageRequest requestParam) {
        Page<KnowledgeDocumentDO> pageParam = new Page<>(requestParam.getCurrent(), requestParam.getSize());
        LambdaQueryWrapper<KnowledgeDocumentDO> queryWrapper = Wrappers.lambdaQuery(KnowledgeDocumentDO.class)
                .eq(KnowledgeDocumentDO::getKbId, kbId)
                .eq(KnowledgeDocumentDO::getDeleted, 0)
                .like(requestParam.getKeyword() != null && !requestParam.getKeyword().isBlank(), KnowledgeDocumentDO::getDocName, requestParam.getKeyword())
                .eq(requestParam.getStatus() != null && !requestParam.getStatus().isBlank(), KnowledgeDocumentDO::getStatus, requestParam.getStatus())
                .orderByDesc(KnowledgeDocumentDO::getCreateTime);

        IPage<KnowledgeDocumentVO> result = documentMapper.selectPage(pageParam, queryWrapper)
                .convert(this::toVO);

        List<String> docIds = result.getRecords().stream()
                .map(KnowledgeDocumentVO::getId)
                .collect(Collectors.toList());
        Set<String> editedDocIds = findEditedDocIds(docIds);
        result.getRecords().forEach(vo -> vo.setChunksEdited(editedDocIds.contains(vo.getId())));

        return result;
    }

    /**
     * DO → VO：摄取配置经 codec 归一化后再出参
     * <p>
     * 库里可能留着旧构建写下的 {@code Integer.MAX_VALUE}（整篇不分块的领域内部哨兵），
     * 而线路上的约定是 {@code -1}。归一化放在出参这一层，旧行不刷库也能正确回显；
     * 列为空表示"走默认"，这个语义要留着，所以空值不在此处物化成一份显式 JSON
     */
    private KnowledgeDocumentVO toVO(KnowledgeDocumentDO documentDO) {
        KnowledgeDocumentVO vo = BeanUtil.toBean(documentDO, KnowledgeDocumentVO.class);
        if (StringUtils.hasText(documentDO.getIngestionSpec())) {
            vo.setIngestionSpec(ingestionSpecCodec.write(ingestionSpecCodec.read(documentDO.getIngestionSpec())));
        }
        return vo;
    }

    private Set<String> findEditedDocIds(List<String> docIds) {
        if (docIds == null || docIds.isEmpty()) {
            return Collections.emptySet();
        }
        QueryWrapper<KnowledgeChunkDO> wrapper = new QueryWrapper<>();
        wrapper.select("DISTINCT doc_id")
                .in("doc_id", docIds)
                .apply("update_time > create_time + INTERVAL 1 SECOND");
        return chunkMapper.selectObjs(wrapper).stream()
                .map(String::valueOf)
                .collect(Collectors.toSet());
    }

    @Override
    public List<KnowledgeDocumentSearchVO> search(String keyword, int limit) {
        if (!StringUtils.hasText(keyword)) {
            return Collections.emptyList();
        }

        int size = Math.min(Math.max(limit, 1), 20);
        Page<KnowledgeDocumentDO> mpPage = new Page<>(1, size);
        LambdaQueryWrapper<KnowledgeDocumentDO> qw = new LambdaQueryWrapper<KnowledgeDocumentDO>()
                .eq(KnowledgeDocumentDO::getDeleted, 0)
                .like(KnowledgeDocumentDO::getDocName, keyword)
                .orderByDesc(KnowledgeDocumentDO::getUpdateTime);

        IPage<KnowledgeDocumentDO> result = documentMapper.selectPage(mpPage, qw);
        List<KnowledgeDocumentSearchVO> records = result.getRecords().stream()
                .map(each -> BeanUtil.toBean(each, KnowledgeDocumentSearchVO.class))
                .toList();
        if (records.isEmpty()) {
            return records;
        }

        Set<String> kbIds = new HashSet<>();
        for (KnowledgeDocumentSearchVO record : records) {
            if (record.getKbId() != null) {
                kbIds.add(record.getKbId());
            }
        }
        if (kbIds.isEmpty()) {
            return records;
        }

        List<KnowledgeBaseDO> bases = knowledgeBaseMapper.selectByIds(kbIds);
        Map<String, String> nameMap = new HashMap<>();
        if (bases != null) {
            for (KnowledgeBaseDO base : bases) {
                nameMap.put(base.getId(), base.getName());
            }
        }
        for (KnowledgeDocumentSearchVO record : records) {
            record.setKbName(nameMap.get(record.getKbId()));
        }
        return records;
    }

    @Override
    @Transactional
    public void enable(String docId, boolean enabled) {
        KnowledgeDocumentDO document = requireEditableDocument(docId);
        int targetEnabled = enabled ? 1 : 0;
        if (Integer.valueOf(targetEnabled).equals(document.getEnabled())) {
            return;
        }
        VectorTarget target = vectorTargetResolver.resolve(knowledgeBaseMapper.selectById(document.getKbId()));
        if (enabled) {
            List<EmbeddedChunk> chunks = knowledgeChunkService.embedPersistedChunks(docId, target);
            if (!chunks.isEmpty()) {
                vectorStoreService.indexDocumentChunks(target.partition(), docId, chunks);
            }
        } else {
            vectorStoreService.deleteDocumentVectors(target.partition(), docId);
        }
        knowledgeChunkService.updateEnabledByDocId(docId, document.getKbId(), enabled);
        documentMapper.update(Wrappers.lambdaUpdate(KnowledgeDocumentDO.class)
                .eq(KnowledgeDocumentDO::getId, docId)
                .set(KnowledgeDocumentDO::getEnabled, targetEnabled)
                .set(KnowledgeDocumentDO::getUpdatedBy, UserContext.getUsername())
                .set(KnowledgeDocumentDO::getUpdateTime, new Date()));
    }

    @Override
    public IPage<KnowledgeDocumentChunkLogVO> getChunkLogs(String docId, Page<KnowledgeDocumentChunkLogVO> page) {
        Page<KnowledgeDocumentChunkLogDO> queryPage = new Page<>(page.getCurrent(), page.getSize());
        return chunkLogMapper.selectPage(queryPage, Wrappers.lambdaQuery(KnowledgeDocumentChunkLogDO.class)
                        .eq(KnowledgeDocumentChunkLogDO::getDocId, docId)
                        .orderByDesc(KnowledgeDocumentChunkLogDO::getCreateTime))
                .convert(this::toChunkLogVO);
    }

    private KnowledgeDocumentChunkLogVO toChunkLogVO(KnowledgeDocumentChunkLogDO entry) {
        KnowledgeDocumentChunkLogVO vo = BeanUtil.toBean(entry, KnowledgeDocumentChunkLogVO.class);
        if (entry.getTotalDuration() != null) {
            long recorded = duration(entry.getExtractDuration()) + duration(entry.getChunkDuration())
                    + duration(entry.getEmbedDuration()) + duration(entry.getPersistDuration());
            vo.setOtherDuration(Math.max(0, entry.getTotalDuration() - recorded));
        }
        return vo;
    }

    private long duration(Long millis) {
        return millis == null ? 0 : millis;
    }

    @Override
    public String preview(String docId) {
        KnowledgeDocumentDO document = requireDocument(documentMapper.selectById(docId));
        if (DisplayType.from(document.getFileType()) != DisplayType.MARKDOWN) {
            throw new ClientException("仅支持预览 Markdown 格式文档");
        }
        return new String(readFileBytes(document), StandardCharsets.UTF_8);
    }

    private void deleteStoredFileQuietly(KnowledgeDocumentDO document) {
        try {
            fileStorageService.deleteByUrl(document.getFileUrl());
        } catch (Exception e) {
            log.warn("删除文档源文件失败，docId={}", document.getId(), e);
        }
    }
}
