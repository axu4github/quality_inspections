package cn.com.centrin.quality.thread;

import cn.com.centrin.base.dbutils.MongoDBClientUtil;
import cn.com.centrin.base.log.BaseSystemOutLog;
import cn.com.centrin.common.function.redis.RedisCacheService;
import cn.com.centrin.quality.log.QualityLogUtils;
import cn.com.centrin.util.StringHelper;
import cn.com.centrin.voice.util.VoiceParamInfo;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.wltea.expression.ExpressionEvaluator;
import org.wltea.expression.datameta.Variable;

import java.util.*;

/**
 * Created by yang on 2016/3/30.
 */
public class QualityRoutineThread extends QualityThread {
    protected Map<String, Boolean> qualityResultMap = new HashMap<String, Boolean>();
    protected QualityResult qualityResult = new QualityResult();
    protected  Map<String,String> experssionRuleMap = new HashMap<>();
    protected  Map<String,String> experssionParamMap = new HashMap<>();
    public QualityRoutineThread(Smartv smartv) {
        super(smartv);
    }

    @Override
    public Object getThreadResult() {
        return qualityResult;
    }

	/**
     * 处理参数质检规则中的 参数 ： 替换为对应的索引值
     * @param allWords
     */
    protected void dealWithExpressionParams(Set<String> allWords) {
        CommonQualityParam cqp = (CommonQualityParam)param;
        List<String> expressionList = new ArrayList<>();
        for (String ex : cqp.getExpressionParams()) {
            expressionList.add(ex.replaceAll("Param",""));
        }
        JSONObject expressionJo = VoiceParamInfo.getParamInfo(
                smartv.getFileName(),smartv.getAreaOfJob(),expressionList);
        if (expressionJo == null || expressionJo.size() == 0){
            expressionJo = new JSONObject();
            for (String el : expressionList){
                expressionJo.put(el,"null");
            }
        }
        Iterator it = expressionJo.keys();
        while (it.hasNext()) {
            String key = String.valueOf(it.next());
            String value = expressionJo.get(key) == null || expressionJo.get(key).toString().trim().equals("")? "null":expressionJo.get(key).toString();
            //将参数质检中关键词加入模型关键词数组
            if (! "null".equals(value)) allWords.addAll(Arrays.asList(value.split(" ")));
            HashMap<String,String> pMap = new HashMap<>();
            pMap.put("key",key);
            value = value.replaceAll(" ","Param");
            if(value.contains("|")){
                pMap.put("value",value.split("\\|")[1]);
                value = value.split("\\|")[0];
            }else{
                pMap.put("value",value);
            }
            smartv.getExperssionRuleParamMap().add(pMap);
            experssionParamMap.put(key,value+"Param"+key);
            // 将experssionRuleMap 中的规则表达式$${手机号}参数替换 eg:一二三 幺二三==》一二三Param幺二三Param手机号
            cqp.getModelExpression().replaceFirst(key,value);
            for (Map.Entry<String, String> entry : cqp.getRuleMap().entrySet()) {
                if(entry.getValue().contains("$${"+key+"}")){
                    if(experssionRuleMap.containsKey(entry.getKey())){
                        experssionRuleMap.put(entry.getKey(),experssionRuleMap.get(entry.getKey()).replace("$${"+key+"}",value+"Param"+key));
                    }else
                        experssionRuleMap.put(entry.getKey(),entry.getValue().replace("$${"+key+"}",value+"Param"+key));
                }
            }
        }
    }

    @Override
    public void run() {
        qualityResult.setThreadStart(Calendar.getInstance().getTimeInMillis());

//        DictionaryManager dictionaryManager=
//                (DictionaryManager) SpringContextUtil.getBean("dictionaryManagerImpl");
//        Constants.NEAR_WORD_SPACE = Integer.valueOf(dictionaryManager.getRedisDataKey("规则逻辑", "NEAR_WORD_SPACE"));
//        Constants.AFTER_WORD_SPACE = Integer.valueOf(dictionaryManager.getRedisDataKey("规则逻辑", "AFTER_WORD_SPACE"));
//        Constants.BEFORE_WORD_SPACE = Integer.valueOf(dictionaryManager.getRedisDataKey("规则逻辑", "BEFORE_WORD_SPACE"));
//        Constants.EACH_WORD_INFO = dictionaryManager.getRedisDataKey("质检任务","单项命中信息");

        CommonQualityParam cqp = (CommonQualityParam)param;
        JSONArray ruleResultInfo = new JSONArray();
        JSONArray factorResultInfo = new JSONArray();
        StringBuffer ruleResultModelBuff = new StringBuffer();
        try {
            // 开始处理 参数质检
            if( cqp.getExpressionParams().size() > 0){
                dealWithExpressionParams(allWords);
            }
            // 处理 参数质检  结束

            Map<String, Object> paramMap = new HashMap<>();
            paramMap.put("_id", getRowKey());
            Map<String, Object> cache = MongoDBClientUtil.findOne(paramMap, "qc");
            JSONObject kwsCache;
            if (cache == null) {
                //没有缓存，现算缓存
                kwsCache = smartv.makeCacheV3AsJson(allWords.toArray(new String[] {}));
            }
            else {
                Object qualityCache = cache.get("qualityCache");
                if (qualityCache.toString().startsWith("V2")) {
                    kwsCache = QualityUtil.trans2Json(qualityCache.toString());
                }
                else {
                    qualityCache = qualityCache.toString().replaceAll("_", "");
                    kwsCache = JSONObject.fromObject(qualityCache);
                }
            }

            for (Map.Entry<String, String> entry : cqp.getRuleMap().entrySet()) {
                ruleResultModelBuff.append(doEachRule(entry.getKey(),
                        experssionRuleMap.containsKey(entry.getKey())
                                ? experssionRuleMap.get(entry.getKey())
                                : entry.getValue(),
                        kwsCache, ruleResultInfo, cqp.getRuleOthersMap().get(entry.getKey())));
            }
            qualityResult.setRuleResultInfo(ruleResultInfo.toString()
                    .replaceAll("\"keyword\"", "\"k\"")
                    .replaceAll("\"times\"", "\"t\"")
                    .replaceAll("\"count\"", "\"c\""));
            qualityResult.setModelResult(ExpressionHandler.getValue(cqp.getModelUnion(), qualityResultMap));
            qualityResult.setFactorResultInfo(filterRepeat(factorResultInfo));
            qualityResult.setModelExpression(cqp.getModelExpression());
            qualityResult.setModelUnion(cqp.getModelUnion());
            qualityResult.setRuleResultModel(ruleResultModelBuff.toString());
            // 处理通用质检
            doCommonRule(cqp);

            //计算模型命中次数
            List<Variable> variables = new ArrayList<>();
            for (Map.Entry<String, Boolean> entry : qualityResultMap.entrySet()) {
                variables.add(Variable.createVariable(entry.getKey().toLowerCase(), true));
            }
            String stackSet = ExpressionEvaluator.compile(
                    cqp.getModelUnion().toLowerCase().replaceAll("or", "||")
                            .replaceAll("and", "&&")
                            .replaceAll("no", "!"), variables);
            Stack<Object> stack = new Stack<>();
            int t1, t2;
            Object tmp;
            for (String s : stackSet.split(" ")) {
                if (s.equals("OR")) {
                    tmp = stack.pop();
                    if (StringUtils.isNumeric(tmp.toString())) t1 = Integer.valueOf(tmp.toString());
                    else t1 = getCountByRule(tmp.toString());
                    tmp = stack.pop();
                    if (StringUtils.isNumeric(tmp.toString())) t2 = Integer.valueOf(tmp.toString());
                    else t2 = getCountByRule(tmp.toString());
                    stack.push(t1 + t2);
                }
                else if (s.equals("AND")) {
                    tmp = stack.pop();
                    if (StringUtils.isNumeric(tmp.toString())) t1 = Integer.valueOf(tmp.toString());
                    else t1 = getCountByRule(tmp.toString());
                    tmp = stack.pop();
                    if (StringUtils.isNumeric(tmp.toString())) t2 = Integer.valueOf(tmp.toString());
                    else t2 = getCountByRule(tmp.toString());

                    if (t1 <= t2) stack.push(t1);
                    else stack.push(t2);
                }
                else if (s.equals("NOT")) {
                    tmp = stack.pop();
                    if (StringUtils.isNumeric(tmp.toString())) t1 = Integer.valueOf(tmp.toString());
                    else t1 = getCountByRule(tmp.toString());
                    if (t1 == 0) stack.push(1);
                    else stack.push(0);
                }
                else stack.push(s);
            }
            if (stack.size() == 1) {
                tmp = stack.pop();
                if (StringUtils.isNumeric(tmp.toString())) {
                    qualityResult.setModelCount(Integer.valueOf(tmp.toString()));
                }
                else qualityResult.setModelCount(getCountByRule(tmp.toString()));
            }
            else {
                QualityLogUtils.init().writeRoutineLog(param.getLoggerId(),param.getLoggerTime(),"QualityRoutineThread","run()","模型计算有误");
                qualityResult.setModelCount(0);
            }

        }catch (Exception e) {
            QualityLogUtils.init().writeRoutineLog(param.getLoggerId(),param.getLoggerTime(),"QualityRoutineThread","run()","error voice : " + smartv.getAreaOfJob() + "_" + smartv.getFileName());
            QualityLogUtils.init().writeRoutineLog(param.getLoggerId(),param.getLoggerTime(),"QualityRoutineThread","run()","处理规则表达式出现错误"+ org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e));
            writeErrorLog("","",e);
            qualityResult.setErrorMsg(e.getMessage());
        }
        this.smartv.setPlainTextA(null);
        this.smartv.setPlainTextB(null);
        this.smartv.setSpeedResultA(null);
        this.smartv.setSpeedResultB(null);
        qualityResult.setThreadEnd(Calendar.getInstance().getTimeInMillis());
//        if((System.currentTimeMillis() / 1000) % 5 == 0){
//            QualityLogUtils.init().writeRoutineLog(param.getLoggerId(),param.getLoggerTime(),"QualityRelatedThread","run()","" + Thread.currentThread().getName() + " done ...");
//        }
    }

    protected int getCountByRule(String ruleAlias) {
        JSONArray ja = JSONArray.fromObject(qualityResult.getRuleResultInfo());
        JSONObject tmpJo;
        for (Object o : ja) {
            tmpJo = JSONObject.fromObject(o);
            if (tmpJo.getString("ruleAlias").toLowerCase().equals(ruleAlias.toLowerCase())) {
                return tmpJo.getInt("riskCount");
            }
        }

        return 0;
    }

    protected String filterRepeat(JSONArray factorResultInfo) {
        Set<String> tmp = new HashSet<>();
        Iterator<String> it;
        String factor;
        JSONArray newArr = new JSONArray();
        JSONObject newJo;
        for (Object eachFactor : factorResultInfo) {
            JSONObject eachFactorJo = JSONObject.fromObject(eachFactor);
            it = eachFactorJo.keys();
            while(it.hasNext()) {
                factor = it.next();
                if (tmp.contains(factor)) continue;
                else {
                    newJo = new JSONObject();
                    newJo.put(factor, eachFactorJo.get(factor));
                    newArr.add(newJo);
                    tmp.add(factor);
                }
            }
        }
        return newArr.toString();
    }

    protected void doEachWord(String keywordAlias, String keywordBase,
                              JSONObject kwsCache,
                              Map<String, Integer> eachFactorSize,
                              JSONArray factorResultInfo, JSONArray factorResultInfoAll,
                              Map<String, Boolean> ruleResultMap,
                              Object[] others,
                              String... type) {
        String keyword1;
        String synonyms1;
        KeyWordCache kwc1;
        List<String> kw1s = new ArrayList<>();

        JSONObject eachKeywordJo = new JSONObject();

        // 处理带参数的规则表达式用索引值替换后 出现多个值的问题
        keywordBase = doExperssionParamAsSynonyms(keywordBase,kw1s);

        //进行角色重置
        if (others != null && others[0] != null) {
            keywordBase = keywordBase.indexOf("-") != -1 ?
                    others[0].toString().toLowerCase().trim() + "-" + keywordBase.split("-")[1] :
                    others[0].toString().toLowerCase().trim() + "-" + keywordBase;
            if (keywordBase.startsWith("-")) keywordBase = keywordBase.substring(1);
        }

        if (keywordBase.split("-").length > 1) {
            keyword1 = keywordBase.split("-")[1];
        }
        else keyword1 = keywordBase;

        synonyms1 = RedisCacheService.initialize().getValueByKey("quality_basic:keyword", keyword1);
        for (String kw : kw1s) {
            synonyms1 = synonyms1 + " " + kw;
        }
        kw1s.clear();
        kw1s.add(keyword1);
        if (! StringHelper.isNull(synonyms1)) kw1s.addAll(Arrays.asList(synonyms1.split(" ")));
        for (String kw : kw1s) {
            if (StringHelper.isNull(kw)) continue;
            if (kwsCache.containsKey(kw)) {
                kwc1 = (KeyWordCache) JSONObject.toBean(
                        JSONObject.fromObject(kwsCache.get(kw)), KeyWordCache.class);
                if (keyword1.equals(kw)) {
                    if (! eachKeywordJo.containsKey(keywordBase)) {
                        eachKeywordJo.put(keywordBase, kwc1.getWordInExpTimesV2(keywordBase, others));
                    }
                }
                else {
                    if (! eachKeywordJo.containsKey(keywordBase + "[" + keywordBase.replaceFirst(keyword1, kw) + "]")) {
                        eachKeywordJo.put(keywordBase + "[" + keywordBase.replaceFirst(keyword1, kw) + "]",
                                kwc1.getWordInExpTimesV2(keywordBase.replaceFirst(keyword1, kw), others));
                    }
                }
                if (kwc1.getWordValueV2(keywordBase.replaceFirst(keyword1, kw), others)) {
                    ruleResultMap.put(keywordAlias, true);
                }
            }
        }

        //以下逻辑覆盖 关键词为预处理后新增词，需要取文本进行再次质检。
        if (! ruleResultMap.containsKey(keywordAlias)) {
            if (! kwsCache.containsKey(keyword1) || (type.length > 0 && type[0].equals("merged"))) {
                //缓存中同义词未命中
                kwc1 = smartv.makeCache(keyword1);

                if (kwc1.getWordValueV2(keywordBase, others)) {
                    ruleResultMap.put(keywordAlias, true);
                    if (! eachKeywordJo.containsKey(keywordBase)) {
                        eachKeywordJo.put(keywordBase, kwc1.getWordInExpTimesV2(keywordBase, others));
                    }
                }
                //同义词关键词
                if (! StringHelper.isNull(synonyms1)) {
                    for (String synonym : synonyms1.split(" ")) {
                        if (StringHelper.isNull(synonym)) continue;
                        if (! kwsCache.containsKey(synonym)) {
                            kwc1 = smartv.makeCache(synonym);
                            if (! eachKeywordJo.containsKey(keywordBase + "[" + keywordBase.replaceFirst(keyword1, synonym) + "]")) {
                                eachKeywordJo.put(keywordBase + "[" + keywordBase.replaceFirst(keyword1, synonym) + "]",
                                        kwc1.getWordInExpTimesV2(keywordBase.replaceFirst(keyword1, synonym), others));
                            }
                            if (kwc1.getWordValueV2(keywordBase.replaceFirst(keyword1, synonym), others)) {
                                ruleResultMap.put(keywordAlias, true);
                            }
                        }
                    }
                }
            }
        }
        if (! ruleResultMap.containsKey(keywordAlias)) {
            ruleResultMap.put(keywordAlias, false);
        }

//        if (others[2] != null && Integer.valueOf(others[2].toString()) != 0) {
//            Iterator it = eachKeywordJo.keys();
//            int count = 0;
//            while(it.hasNext()) {
//                count = count + JSONArray.fromObject(eachKeywordJo.get(it.next())).size();
//                if (count < Integer.valueOf(others[2].toString())) {
//                    ruleResultMap.put(keywordAlias, false);
//                }
//            }
//        }

        Iterator it = eachKeywordJo.keys();
        int count = 0;
        while(it.hasNext()) {
            count = count + JSONArray.fromObject(eachKeywordJo.get(it.next())).size();
        }
        eachFactorSize.put(keywordAlias, count);

        factorResultInfo.add(eachKeywordJo);
        factorResultInfoAll.add(eachKeywordJo);
    }

    protected void doEachExp(String expAlias, String exp,
                             JSONObject kwsCache,
                             Map<String, Integer> eachFactorSize,
                             JSONArray factorResultInfo,JSONArray factorResultInfoAll,
                             Map<String, Boolean> ruleResultMap,
                             Object[] others) {
        String newFactor;
        String keyword1, keyword2;
        String synonyms1, synonyms2;
        KeyWordCache kwc1, kwc2;
        List<String> kw1s = new ArrayList<>(), kw2s = new ArrayList<>();
        List<String> allPositions = new ArrayList<>();
        JSONObject eachFactorJo = new JSONObject();
        JSONObject eachWordInExpJo = new JSONObject();
        int number = 0;
        String[] tempKeywords = exp.split(
                Constants.EXPRESSION_OPERATOR_AFTER +
                        "|" +  Constants.EXPRESSION_OPERATOR_BEFORE +
                        "|" +  Constants.EXPRESSION_OPERATOR_NEAR);
        String kw1WithRole = tempKeywords[0].replaceAll("\\(", "").replaceAll("\\)", "");
        String kw2WithRole = tempKeywords[1].replaceAll("\\(", "").replaceAll("\\)", "");
        newFactor = exp;
        if (others != null && others[0] != null) {
            newFactor = exp.replaceFirst(kw1WithRole, "").replaceFirst(kw2WithRole, "");
            kw1WithRole = kw1WithRole.split("-").length > 1 ?
                    others[0].toString().toLowerCase().trim() + "-" + kw1WithRole.split("-")[1] :
                    others[0].toString().toLowerCase().trim() + "-" + kw1WithRole;
            if (kw1WithRole.startsWith("-")) kw1WithRole = kw1WithRole.substring(1);

            kw2WithRole = kw2WithRole.split("-").length > 1 ?
                    others[0].toString().toLowerCase().trim() + "-" + kw2WithRole.split("-")[1] :
                    others[0].toString().toLowerCase().trim() + "-" + kw2WithRole;
            if (kw2WithRole.startsWith("-")) kw2WithRole = kw2WithRole.substring(1);
            newFactor = kw1WithRole + newFactor + kw2WithRole;
        }

        exp = doExperssionParamForExp(kw1WithRole,exp);
        exp = doExperssionParamForExp(kw2WithRole,exp);
        // 处理带参数的规则表达式用索引值替换后 出现多个值的问题
        kw1WithRole = doExperssionParamAsSynonyms(kw1WithRole,kw1s);
        kw2WithRole = doExperssionParamAsSynonyms(kw2WithRole,kw2s);

        if (kw1WithRole.split("-").length > 1) keyword1 = kw1WithRole.split("-")[1];
        else keyword1 = kw1WithRole;
        if (kw2WithRole.split("-").length > 1) keyword2 = kw2WithRole.split("-")[1];
        else keyword2 = kw2WithRole;

        synonyms1 = RedisCacheService.initialize().getValueByKey("quality_basic:keyword", keyword1);
        for (String kw : kw1s){
            synonyms1 = synonyms1 + " " + kw;
        }
        kw1s.clear();
        kw1s.add(keyword1);
        if (! StringHelper.isNull(synonyms1)) kw1s.addAll(Arrays.asList(synonyms1.split(" ")));
        synonyms2 = RedisCacheService.initialize().getValueByKey("quality_basic:keyword", keyword2);
        for (String kw : kw2s){
            synonyms2 = synonyms2 + " " + kw;
        }
        kw2s.clear();
        kw2s.add(keyword2);
        if (! StringHelper.isNull(synonyms2)) kw2s.addAll(Arrays.asList(synonyms2.split(" ")));
        Set<Integer> ins = new HashSet<>();
        for (String kw1 : kw1s) {
            if (StringHelper.isNull(kw1)) continue;
            if (kwsCache.containsKey(kw1)) {
                number++; //用于判断第二个关键词组是第几次循环
                kwc1 = (KeyWordCache) JSONObject.toBean(
                        JSONObject.fromObject(kwsCache.get(kw1)), KeyWordCache.class);
                //保留全部表达式中的单个关键词的命中信息start
                if (keyword1.equals(kw1)) {
                    if (! eachWordInExpJo.containsKey(kw1WithRole)) {
                        eachWordInExpJo.put(kw1WithRole, kwc1.getWordInExpTimesV2(kw1WithRole, others));
                    }
                }
                else {
                    if (! eachWordInExpJo.containsKey(kw1WithRole + "[" + kw1WithRole.replaceAll(keyword1, kw1) + "]")) {
                        eachWordInExpJo.put(kw1WithRole + "[" + kw1WithRole.replaceAll(keyword1, kw1) + "]",
                                kwc1.getWordInExpTimesV2(kw1WithRole.replaceAll(keyword1, kw1), others));
                    }
                }

                //保留全部表达式中的单个关键词的命中信息end
                for (String kw2 : kw2s) {
                    if (StringHelper.isNull(kw2)) continue;
                    if (kwsCache.containsKey(kw2)) {
                        kwc2 = (KeyWordCache) JSONObject.toBean(
                                JSONObject.fromObject(kwsCache.get(kw2)), KeyWordCache.class);
                        //保留全部表达式中的单个关键词的命中信息start
                        if (number == 1) {
                            if (keyword2.equals(kw2)) {
                                if (!eachWordInExpJo.containsKey(kw2WithRole)) {
                                    eachWordInExpJo.put(kw2WithRole, kwc2.getWordInExpTimesV2(kw2WithRole, others));
                                }
                            } else {
                                if (!eachWordInExpJo.containsKey(kw2WithRole + "[" + kw2WithRole.replaceAll(keyword2, kw2) + "]")) {
                                    eachWordInExpJo.put(kw2WithRole + "[" + kw2WithRole.replaceAll(keyword2, kw2) + "]",
                                            kwc2.getWordInExpTimesV2(kw2WithRole.replaceAll(keyword2, kw2), others));
                                }
                            }
                        }
                        //保留全部表达式中的单个关键词的命中信息end
                        newFactor = getNewFactor(newFactor, keyword1, kw1, keyword2, kw2);
                        allPositions.addAll(kwc1.getExpFactorTimesV2(newFactor, kwc2, others, ins));
                        if (kwc1.getExpFactorValueV2(newFactor, kwc2, others)) {
                            ruleResultMap.put(expAlias, true);
                        }
                    }
                }
            }
        }
        ins.clear();
        number = 0;
        if (! ruleResultMap.containsKey(expAlias)) {
            //keyword1为新增词 或 keyword2为新增词
            if (! kwsCache.containsKey(keyword1) || ! kwsCache.containsKey(keyword2)) {
//                aTimes.clear();
//                bTimes.clear();
                for (String kw1 : kw1s) {
                    if (StringHelper.isNull(kw1)) continue;
                    number++;
                    if (kwsCache.containsKey(kw1)) {
                        kwc1 = (KeyWordCache) JSONObject.toBean(
                                JSONObject.fromObject(kwsCache.get(kw1)), KeyWordCache.class);
                    }
                    else {
                        kwc1 = smartv.makeCache(kw1);
                    }

                    //保留全部表达式中的单个关键词的命中信息start
                    if (keyword1.equals(kw1)) {
                        if (!eachWordInExpJo.containsKey(kw1WithRole)) {
                            eachWordInExpJo.put(kw1WithRole, kwc1.getWordInExpTimesV2(kw1WithRole, others));
                        }
                    } else {
                        if (!eachWordInExpJo.containsKey(kw1WithRole + "[" + kw1WithRole.replaceAll(keyword1, kw1) + "]")) {
                            eachWordInExpJo.put(kw1WithRole + "[" + kw1WithRole.replaceAll(keyword1, kw1) + "]",
                                    kwc1.getWordInExpTimesV2(kw1WithRole.replaceAll(keyword1, kw1), others));
                        }
                    }

                    //保留全部表达式中的单个关键词的命中信息end

                    for (String kw2 : kw2s) {
                        if (StringHelper.isNull(kw2)) continue;
                        if (kwsCache.containsKey(kw2)) {
                            kwc2 = (KeyWordCache) JSONObject.toBean(
                                    JSONObject.fromObject(kwsCache.get(kw2)), KeyWordCache.class);
                        }
                        else {
                            kwc2 = smartv.makeCache(kw2);
                        }
                        //保留全部表达式中的单个关键词的命中信息start
                        if (number == 1) {
                            if (keyword2.equals(kw2)) {
                                if (!eachWordInExpJo.containsKey(kw2WithRole)) {
                                    eachWordInExpJo.put(kw2WithRole, kwc2.getWordInExpTimesV2(kw2WithRole, others));
                                }
                            } else {
                                if (!eachWordInExpJo.containsKey(kw2WithRole + "[" + kw2WithRole.replaceAll(keyword2, kw2) + "]")) {
                                    eachWordInExpJo.put(kw2WithRole + "[" + kw2WithRole.replaceAll(keyword2, kw2) + "]",
                                            kwc2.getWordInExpTimesV2(kw2WithRole.replaceAll(keyword2, kw2), others));
                                }
                            }
                        }
                        //保留全部表达式中的单个关键词的命中信息end
                        newFactor = getNewFactor(newFactor, keyword1, kw1, keyword2, kw2);
                        allPositions.addAll(kwc1.getExpFactorTimesV2(newFactor, kwc2, others, ins));
                        if (kwc1.getExpFactorValueV2(newFactor, kwc2, others)) {
                            ruleResultMap.put(expAlias, true);
                        }
                    }
                }
            }
        }
        ins.clear();
        if (! ruleResultMap.containsKey(expAlias)) {
            ruleResultMap.put(expAlias, false);
        }

//        if (others[2] != null && Integer.valueOf(others[2].toString()) != 0) {
//            if (allPositions.size() < Integer.valueOf(others[2].toString())) {
//                ruleResultMap.put(expAlias, false);
//            }
//        }

        eachFactorSize.put(expAlias, allPositions.size());

        eachFactorJo.put(exp, allPositions);
        factorResultInfo.add(eachFactorJo);
        //添加结果到因子时间结果中
        factorResultInfoAll.add(eachFactorJo);
        JSONObject eachWordInExpJo2 = new JSONObject();
        Iterator iterator = eachWordInExpJo.keys();
        while(iterator.hasNext()){
            String key = (String) iterator.next();
            eachWordInExpJo2.put(key,eachWordInExpJo.getString(key));
        }
        mergeKeywordTimes(factorResultInfoAll, eachWordInExpJo2);
        if(!Constants.EACH_WORD_INFO.equals("unkeep")) {
            mergeKeywordTimes(factorResultInfo, eachWordInExpJo);
        }else{
            mergeKeywordTimes(factorResultInfo, new JSONObject());
        }
    }

//    public static void main(String[] args) {
//        List<String[]> allKws = new ArrayList<>();
//        String[] k1 = {"a-银行", "a-网上银行", "a-支行"};
//        String[] k2 = {"b-密码", "b-密钥", "b-口令"};
//        String[] k3 = {"存折", "银行卡", "卡片"};
//        allKws.add(k1);allKws.add(k2);allKws.add(k3);
//
//        String[] tempKeywords = {"a-银行", "b-密码", "存折"};
//        JSONObject kwsCache = new JSONObject();
//
//        KeyWordCache c1 = new KeyWordCache();
//        c1.setKeyword("银行");
//        RoleCache c1r1 = new RoleCache();
//        c1r1.setPositions(new Integer[]{10, 15, 20});
//        c1r1.setTimes(new String[]{"1", "2", "3"});
//        c1r1.setCount(3);
//        c1.setService(c1r1);
//
//        RoleCache c1r2 = new RoleCache();
//        c1r2.setPositions(new Integer[]{25});
//        c1r2.setTimes(new String[]{"4"});
//        c1r2.setCount(1);
//        c1.setCustomer(c1r2);
//
//        kwsCache.put("银行", c1);
//
//        KeyWordCache c2 = new KeyWordCache();
//        c2.setKeyword("支行");
//        RoleCache c2r1 = new RoleCache();
//        c2r1.setPositions(new Integer[]{30, 35});
//        c2r1.setTimes(new String[]{"5", "6"});
//        c2r1.setCount(2);
//        c2.setService(c2r1);
//
//        kwsCache.put("支行", c2);
//
//        //-----------------------
//        KeyWordCache c3 = new KeyWordCache();
//        c3.setKeyword("密码");
//        RoleCache c3r1 = new RoleCache();
//        c3r1.setPositions(new Integer[]{33});
//        c3r1.setTimes(new String[]{"7"});
//        c3r1.setCount(1);
//        c3.setService(c3r1);
//
//        RoleCache c3r2 = new RoleCache();
//        c3r2.setPositions(new Integer[]{28, 38});
//        c3r2.setTimes(new String[]{"9", "10"});
//        c3r2.setCount(2);
//        c3.setCustomer(c3r2);
//
//        kwsCache.put("密码", c3);
//
//        //----------
//
//        KeyWordCache c4 = new KeyWordCache();
//        c4.setKeyword("存折");
//        RoleCache c4r1 = new RoleCache();
//        c4r1.setPositions(new Integer[]{40});
//        c4r1.setTimes(new String[]{"11"});
//        c4r1.setCount(1);
//        c4.setService(c4r1);
//
//        RoleCache c4r2 = new RoleCache();
//        c4r2.setPositions(new Integer[]{45, 48});
//        c4r2.setTimes(new String[]{"12", "13"});
//        c4r2.setCount(2);
//        c4.setCustomer(c4r2);
//
//        kwsCache.put("存折", c4);
//
//        String firstOne;
//        String updateOne;
//        KeyWordCache cache;
//
//        StringBuffer recuResult = new StringBuffer();
//        List<String> result = new ArrayList<>();
//
//        for (int i = 0; i < allKws.get(0).length; i++) {
//
//            firstOne = allKws.get(0)[i];
//            if (firstOne.split("-").length > 1) updateOne = firstOne.split("-")[1];
//            else updateOne = firstOne;
//
//            if (kwsCache.containsKey(updateOne)) {
//                cache = (KeyWordCache) JSONObject.toBean(
//                        JSONObject.fromObject(kwsCache.get(updateOne)), KeyWordCache.class);
//                for (String str : cache.getWordInExpTimes(firstOne)) {
//                    recuResult.append(firstOne);
//                    recuResult.append("_");
//                    recuResult.append(str);
//                    if (recu(recuResult, 1, allKws, kwsCache)) {
//                        result.add(recuResult.toString());
//                    }
//                    recuResult.delete(0, recuResult.length());
//
//                }
//            }
//        }
//        for (String s : result) {
//            System.out.println(s);
//        }
//    }

//    public static boolean recu(StringBuffer recuResult, int index, List<String[]> allKws, JSONObject kwsCache) {
//        StringBuffer sb = new StringBuffer();
//        String[] tmp;
//        String currentKw, lastKw;
//        String lastKwAndRole, last;
//        KeyWordCache currentCache, lastCache;
//        List<String> tmpTimes;
//        for (String currentKwAndRole : allKws.get(index)) {
//            if (currentKwAndRole.split("-").length > 1) {
//                currentKw = currentKwAndRole.split("-")[1];
//            }
//            else currentKw = currentKwAndRole;
//
//            tmp = recuResult.toString().split(",");
//            last = tmp[tmp.length - 1];
//            lastKwAndRole = tmp[tmp.length - 1].split("_")[0];
//            if (lastKwAndRole.split("-").length > 1) {
//                lastKw = lastKwAndRole.split("-")[1];
//            }
//            else lastKw = lastKwAndRole;
//
//            if (kwsCache.containsKey(currentKw)) {
//                currentCache = (KeyWordCache) JSONObject.toBean(
//                        JSONObject.fromObject(kwsCache.get(currentKw)), KeyWordCache.class);
//            }
//            else {
//                return false;
//            }
//
//            if (kwsCache.containsKey(lastKw)) {
//                lastCache = (KeyWordCache) JSONObject.toBean(
//                        JSONObject.fromObject(kwsCache.get(lastKw)), KeyWordCache.class);
//            }
//            else return false;
//
//            if (index == allKws.size() - 1) {
//                tmpTimes = lastCache.getBunchFactorTimes(last + "after" + currentKwAndRole, currentCache);
//                //按现在的处理逻辑, 只取最先找到的时间点, 暂保留for循环,所以用了break,方便以后修改
//                for (String time : tmpTimes) {
//                    recuResult.append(",");
//                    recuResult.append(currentKwAndRole);
//                    recuResult.append("_");
//                    recuResult.append(time);
//                    return true;
//                }
//            }
//            else {
//                tmpTimes = lastCache.getBunchFactorTimes(last + "after" + currentKwAndRole, currentCache);
//                for (String time : tmpTimes) {
//                    recuResult.append(",");
//                    recuResult.append(currentKwAndRole);
//                    recuResult.append("_");
//                    recuResult.append(time);
//                    if (recu(recuResult, index + 1, allKws, kwsCache)) return true;
//                }
//                return false;
//            }
//        }
//        return false;
//    }

//    protected void doEachBunch(String expAlias, String exp,
//                             JSONObject kwsCache,
//                             JSONArray factorResultInfo,
//                             Map<String, Boolean> ruleResultMap) {
////        logger.info("expAlias : " + expAlias);
////        logger.info("exp : " + exp);
////        logger.info("kwsCache : " + kwsCache.toString());
//
//        String tmp;
//        //表达式中关键词（带角色)
//        String[] tempKeywords = exp.replaceAll("\\(", "").replaceAll("\\)", "").split(",");
//        //表达式中关键词（不带角色)
//        String[] keywords = new String[tempKeywords.length];
//        //同义词列表
//        List<String[]> kwSynonyms = new ArrayList<>();
//        //缓存对象
//        KeyWordCache[] kwCaches = new KeyWordCache[tempKeywords.length];
//        //每个关键词的命中详情
//        JSONObject eachWordInExpJo = new JSONObject();
//
//        for (int i = 0; i < tempKeywords.length; i++) {
//            if (tempKeywords[i].split("-").length > 1) keywords[i] = tempKeywords[i].split("-")[1];
//            else keywords[i] = tempKeywords[i];
//
//            tmp = RedisCacheService.initialize().getValueByKey("quality_basic_keyword", keywords[i]);
//            if (StringHelper.isNull(tmp)) tmp = keywords[i];
//            else tmp = keywords[i] + " " + tmp;
//
//            kwSynonyms.add(tmp.split(" "));
//        }
//
//        for (int j = 0; j < tempKeywords.length; j++) {
//            //提取每个关键词的缓存对象写入数组
//            if (kwsCache.containsKey(keywords[j])) {
//                kwCaches[j] = (KeyWordCache) JSONObject.toBean(
//                        JSONObject.fromObject(kwsCache.get(keywords[j])), KeyWordCache.class);
//            }
//            else {
//                kwCaches[j] = smartv.makeCache(keywords[j]);
//            }
//            //处理原始词及同义词的命中详情
//            for (int k = 0; k < kwSynonyms.get(j).length; k++) {
//                if (k == 0) {
//                    //原始词
//                    if (! eachWordInExpJo.containsKey(tempKeywords[j])) {
//                        eachWordInExpJo.put(tempKeywords[j], kwCaches[j].getWordInExpTimes(tempKeywords[j]));
//                    }
//                }
//                else {
//                    if (! eachWordInExpJo.containsKey(
//                            tempKeywords[j] + "[" + tempKeywords[j].replaceAll(keywords[j], kwSynonyms.get(j)[k]) + "]")) {
//                        eachWordInExpJo.put(tempKeywords[j] + "[" +
//                                tempKeywords[j].replaceAll(keywords[j], kwSynonyms.get(j)[k]) + "]",
//                                kwCaches[j].getWordInExpTimes(tempKeywords[j].replaceAll(keywords[j], kwSynonyms.get(j)[k])));
//                    }
//                }
//            }
//        }
//
////        ruleResultMap.put(expAlias, getBoundFactorValue(kwSynonyms, tempKeywords, kwsCache));
//
//
//        for (int i = 0; i < kwSynonyms.size(); i++) {
//            for (int j = 0; j < kwSynonyms.get(i).length; j++) {
//                kwSynonyms.get(i)[j] = tempKeywords[i].replaceFirst(keywords[i], kwSynonyms.get(i)[j]);
////                logger.info("new kw : " + kwSynonyms.get(i)[j]);
//            }
//        }
//
//        List<String> allPositions = new ArrayList<>();
//
//        String firstOne;
//        String updateOne;
//        KeyWordCache cache;
//
//        StringBuffer recuResult = new StringBuffer();
//        List<String> result = new ArrayList<>();
//
//        for (int i = 0; i < kwSynonyms.get(0).length; i++) {
//
//            firstOne = kwSynonyms.get(0)[i];
//            if (firstOne.split("-").length > 1) updateOne = firstOne.split("-")[1];
//            else updateOne = firstOne;
//
//            if (kwsCache.containsKey(updateOne)) {
//                cache = (KeyWordCache) JSONObject.toBean(
//                        JSONObject.fromObject(kwsCache.get(updateOne)), KeyWordCache.class);
//                for (String str : cache.getWordInExpTimes(firstOne)) {
//                    recuResult.append(firstOne);
//                    recuResult.append("_");
//                    recuResult.append(str);
//                    if (recu(recuResult, 1, kwSynonyms, kwsCache)) {
//                        result.add(recuResult.toString());
//                    }
//                    recuResult.delete(0, recuResult.length());
//
//                }
//            }
//        }
//
//        StringBuffer sb = new StringBuffer();
//        for (String s : result) {
//            sb.delete(0, sb.length());
//            for (String x : s.split(",")) {
//                sb.append(",");
//                sb.append(x.split("_")[1]);
//            }
//            allPositions.add(sb.substring(1));
//        }
//
//        JSONObject eachFactorJo = new JSONObject();
//
//        eachFactorJo.put(exp, allPositions);
//        factorResultInfo.add(eachFactorJo);
//        //添加结果到因子时间结果中
//        mergeKeywordTimes(factorResultInfo, eachWordInExpJo);
//    }

    protected void doEachBunchV2(String expAlias, String exp,
                                 JSONObject kwsCache, Map<String, Integer> eachFactorSize,
                                 JSONArray factorResultInfo,JSONArray factorResultInfoAll,
                                 Map<String, Boolean> ruleResultMap,
                                 Object[] others) {

//        Object[] others = new Object[] {"", "30-80", 2, 10};

//        logger.info("expAlias : " + expAlias);
//        logger.info("exp : " + exp);
//        logger.info("kwsCache : " + kwsCache.toString());

        String tmp, currentKw;
        //表达式中关键词（带角色)
        String[] tempKeywords = exp.replaceAll("\\(", "").replaceAll("\\)", "").split(",");
        //同义词列表(带角色)
        List<String[]> kwSynonyms = new ArrayList<>();
        //每个关键词的命中详情
        JSONObject eachWordInExpJo = new JSONObject();
        KeyWordCache cache;
        for (int i = 0; i < tempKeywords.length; i++) {
            if (tempKeywords[i].split("-").length > 1) currentKw = tempKeywords[i].split("-")[1];
            else currentKw = tempKeywords[i];

            List<String> kwls = new ArrayList<>();
            exp = doExperssionParamForExp(currentKw,exp);
            currentKw = doExperssionParamAsSynonyms(currentKw,kwls);

            tmp = RedisCacheService.initialize().getValueByKey("quality_basic:keyword", currentKw);

            for (String kw : kwls){
                tmp = tmp + " "+ kw;
            }
            kwls.clear();
            if (StringHelper.isNull(tmp)) {
                if (others != null && others[0] != null) {
                    tmp = tempKeywords[i].split("-").length > 1 ?
                            others[0].toString().toLowerCase().trim() + "-" + tempKeywords[i].split("-")[1] :
                            others[0].toString().toLowerCase().trim() + "-" + tempKeywords[i];
                    if (tmp.startsWith("-")) tmp = tmp.substring(1);
//                    logger.info("if each kwSynonyms : " + tmp);
                    kwSynonyms.add(tmp.split(" "));
                }
                else {
                    kwSynonyms.add(tempKeywords[i].split(" "));
                }
            }
            else {
                if (tempKeywords[i].split("-").length > 1) {
                    if (others != null && others[0] != null) {
                        tmp = tempKeywords[i] + " " + tempKeywords[i].split("-")[0] + "-" +
                                tmp.replaceAll(" ", " " + tempKeywords[i].split("-")[0] + "-");
                        tmp.replaceAll(tempKeywords[i].split("-")[0] + "-",
                                others[0].toString().toLowerCase().trim() + "-");
                        if (tmp.startsWith("-")) tmp.replaceAll("-", "");
                    }
                    else {
                        tmp = tempKeywords[i] + " " + tempKeywords[i].split("-")[0] + "-" +
                                tmp.replaceAll(" ", " " + tempKeywords[i].split("-")[0] + "-");
                    }
                }
                else {
                    if (others != null && others[0] != null) {
                        tmp = currentKw + " " + tmp;
                        tmp = others[0].toString().toLowerCase().trim() + "-" + tmp;
                        tmp = tmp.replaceAll(" ", " " + others[0].toString().toLowerCase().trim() + "-");

                        if (tmp.startsWith("-")) tmp = tmp.replaceAll("-", "");
                    }
                    else tmp = currentKw + " " + tmp;
                }

                kwSynonyms.add(tmp.split(" "));
//                logger.info("else each kwSynonyms : " + tmp);
            }
        }

        for (int j = 0; j < tempKeywords.length; j++) {
            //处理原始词及同义词的命中详情
            for (int k = 0; k < kwSynonyms.get(j).length; k++) {
                if (kwSynonyms.get(j)[k].split("-").length > 1) currentKw = kwSynonyms.get(j)[k].split("-")[1];
                else currentKw = kwSynonyms.get(j)[k];
                if (k == 0) {
                    //原始词
                    if (! eachWordInExpJo.containsKey(tempKeywords[j])) {
                        if (kwsCache.containsKey(currentKw)) {
//                            logger.info("old...............");
                            cache = (KeyWordCache) JSONObject.toBean(
                                    JSONObject.fromObject(kwsCache.get(currentKw)), KeyWordCache.class);
//                            logger.info("currentKw : " + currentKw);
//                            logger.info("3333333333333333333333333 : " + JSONObject.fromObject(kwsCache.get(currentKw)).toString());
                        }
                        else {
//                            logger.info("make .............");
                            cache = smartv.makeCache(currentKw);
                        }
//                        logger.info("smartv.getPlainTextA : " + smartv.getPlainTextA());
//                        logger.info("smartv.getPlainTextB : " + smartv.getPlainTextB());
//                        logger.info("smartv.getSpeedResultA : " + smartv.getSpeedResultA());
//                        logger.info("smartv.getSpeedResultB : " + smartv.getSpeedResultB());
//                        logger.info("currentKw : " + currentKw);
//                        logger.info(" cache : " + JSONObject.fromObject(cache));
                        eachWordInExpJo.put(tempKeywords[j], cache.getWordInExpTimesV2(kwSynonyms.get(j)[k], others));
//                        if (smartv.getIsRelated()) {
//                            logger.info("exptimes : " + eachWordInExpJo.getString(tempKeywords[j]));
//                        }
                    }
                }
                else {
                    if (! eachWordInExpJo.containsKey(
                            tempKeywords[j] + "[" + kwSynonyms.get(j)[k] + "]")) {
                        if (kwsCache.containsKey(currentKw)) {
                            cache = (KeyWordCache) JSONObject.toBean(
                                    JSONObject.fromObject(kwsCache.get(currentKw)),
                                    KeyWordCache.class);
//                            logger.info("44444444444444444 : " + JSONObject.fromObject(kwsCache.get(currentKw)));
                        }
                        else {
                            cache = smartv.makeCache(currentKw);
                        }
//                        logger.info("else cache : " + JSONObject.fromObject(cache));
                        eachWordInExpJo.put(tempKeywords[j] + "[" + kwSynonyms.get(j)[k] + "]",
                                cache.getWordInExpTimesV2(kwSynonyms.get(j)[k], others));
                    }
                }
            }
        }

        List<Integer[]> kwSynonymsPositions = new ArrayList<>();
        List<String[]> kwSynonymsTimes = new ArrayList<>();
        List<String[]> allSynonyms = new ArrayList<>();
        List<Integer> tempResult;
        for (int i = 0; i < kwSynonyms.size(); i++) {
            List<Integer> eachKeywordPositions = new ArrayList<>();
            List<String> eachKeywordTimes = new ArrayList<>();
            List<String> eachSynonyms = new ArrayList<>();
            for (int j = 0; j < kwSynonyms.get(i).length; j++) {
                if (kwSynonyms.get(i)[j].split("-").length > 1) {
                    if (kwsCache.containsKey(kwSynonyms.get(i)[j].split("-")[1])) {
                        cache = (KeyWordCache) JSONObject.toBean(
                                JSONObject.fromObject(kwsCache.get(kwSynonyms.get(i)[j].split("-")[1])), KeyWordCache.class);
                    }
                    else {
                        cache = smartv.makeCache(kwSynonyms.get(i)[j].split("-")[1]);
                    }

                }
                else {
                    if (kwsCache.containsKey(kwSynonyms.get(i)[j])) {
                        cache = (KeyWordCache) JSONObject.toBean(
                                JSONObject.fromObject(kwsCache.get(kwSynonyms.get(i)[j])), KeyWordCache.class);
//                        if (smartv.getIsRelated()) {
//                            logger.info("sfasdfasd : " + JSONObject.fromObject(kwsCache.get(kwSynonyms.get(i)[j])).toString());
//                        }
                    }
                    else {
                        cache = smartv.makeCache(kwSynonyms.get(i)[j]);
                    }
                }
                tempResult = cache.getPositionsV2(kwSynonyms.get(i)[j], others);
                eachKeywordTimes.addAll(cache.getWordInExpTimesV2(kwSynonyms.get(i)[j], others));
                eachKeywordPositions.addAll(tempResult);

                for (int k = 0; k < tempResult.size(); k++) {
                    if (kwSynonyms.get(i)[j].split("-").length > 1) {
                        eachSynonyms.add(kwSynonyms.get(i)[j].split("-")[1]);
                    }
                    else eachSynonyms.add(kwSynonyms.get(i)[j]);
                }
            }

            kwSynonymsPositions.add(eachKeywordPositions.toArray(new Integer[]{}));
            kwSynonymsTimes.add(eachKeywordTimes.toArray(new String[]{}));
            allSynonyms.add(eachSynonyms.toArray(new String[]{}));
        }

//        if (smartv.getIsRelated()) {
//            for (int i = 0; i < kwSynonymsPositions.size(); i++) {
//                for (int j = 0; j < kwSynonymsPositions.get(i).length; j++) {
//                    logger.info("i : " + i + ", j : " + j + ", value : " + kwSynonymsPositions.get(i)[j]);
//                }
//            }
//
//            logger.info("-------------------------");
//
//            for (int i = 0; i < kwSynonymsTimes.size(); i++) {
//                for (int j = 0; j < kwSynonymsTimes.get(i).length; j++) {
//                    logger.info("i : " + i + ", j : " + j + ", value : " + kwSynonymsTimes.get(i)[j]);
//                }
//            }
//        }


        List<String> allPositions = new ArrayList<>();
        StringBuffer timeSb = new StringBuffer();
        StringBuffer positionSb = new StringBuffer();
        boolean flag = false;
        int currentOne, currentLen;
        //记录每一层计算到第几个
        int[] currentIndexs = new int[kwSynonymsPositions.size()];
        //初始化数组，默认被对比的数组从0开始
        for (int i = 0; i < kwSynonymsPositions.size(); i++) {
            currentIndexs[i] = 0;
        }
        int t1 = Integer.MIN_VALUE;
        int t2 = Integer.MAX_VALUE;
        int distance;
        if (others != null && others[1] != null) {
            t1 = Integer.valueOf(others[1].toString().split("-")[0]);
            t2 = Integer.valueOf(others[1].toString().split("-")[1]);
        }
        if (others != null && others[3] != null) {
            distance = Integer.valueOf(others[3].toString());
        }
        else distance = Constants.AFTER_WORD_SPACE;

        // 判断是否有关键词没有出现（位置点数组为null的）
        boolean isContinue = true;
        if(kwSynonymsPositions.size() == 0){
            isContinue = false;
        }else{
            for (Integer[] eachPositions : kwSynonymsPositions){
                if(eachPositions == null || eachPositions.length == 0){
                    isContinue = false;
                }
            }
        }

        if(isContinue){
//            if (smartv.getIsRelated()) logger.info("continue .......");
            for (int i = 0; i < kwSynonymsPositions.get(0).length; i++) {

                // 判断关键词是否在规定时间范围内
                if (Float.valueOf(kwSynonymsTimes.get(0)[i]) < t1 ||
                        Float.valueOf(kwSynonymsTimes.get(0)[i]) > t2) {
                    continue;
                }

                //最多执行a[]的长度次
                currentIndexs[0] = i;
                currentOne = kwSynonymsPositions.get(0)[i];
                currentLen = allSynonyms.get(0)[i].length();
                timeSb.append(kwSynonymsTimes.get(0)[i]);
                positionSb.append(currentOne);
                for (int j = 1; j < kwSynonymsPositions.size(); j++) {
                    //比较a[i]与 all.get(j)的每一个距离  最多执行表达式中包含的关键词个数次
                    for (int k = currentIndexs[j]; k < kwSynonymsPositions.get(j).length; k++) {
                        //测试=>改为计算距离
//                        if (smartv.getIsRelated()) {
//                            logger.info("for j :" + j + ", k : " + k + ", currentOne : " + currentOne + ", kwSynonymsPositions.get(j)[k] : " + kwSynonymsPositions.get(j)[k]);
//                        }
//                        for j :2, k : 0, currentOne : 6, kwSynonymsPositions.get(j)[k] : 8
                        if ((currentOne + currentLen) <= kwSynonymsPositions.get(j)[k] &&
                                kwSynonymsPositions.get(j)[k] - (currentOne + currentLen)
                                        <= distance) {
                            timeSb.append(",");
                            timeSb.append(kwSynonymsTimes.get(j)[k]);
                            positionSb.append(",");
                            positionSb.append(kwSynonymsPositions.get(j)[k]);

//                            if (smartv.getIsRelated()) {
//                                logger.info("timeSb: " + timeSb.toString());
//                                logger.info("positionSb: " + positionSb.toString());
//                            }



                            if (canContinue(timeSb, positionSb, kwSynonymsPositions.size())) {
//                                if (smartv.getIsRelated()) {
//                                    logger.info("timeSb *** : " + timeSb.toString());
//                                    logger.info("positionSb *** : " + positionSb.toString());
//
//                                    logger.info("i : " + i + ", j : " + j + ", k :" + k);
//                                }

                                flag = true;
                                currentIndexs[j] = k;
                                currentOne = kwSynonymsPositions.get(j)[k];
                                currentLen = allSynonyms.get(j)[k].length();
                                break;
                            }
                        }
                    }
                    if (flag) {
                        flag = false;
                        //进入下一层计算
                        if (j + 1 == kwSynonymsPositions.size()) {
                            //最后一层命中，则整个因子命中
                            ruleResultMap.put(expAlias, true);
                        }
                    }
                    else {
                        //退回到上一层
                        if (j - 1 == 0) {
                            //退出当前a[i], 并将a[i]从buffer中去掉。
                            if (timeSb.lastIndexOf("|") != -1) {
                                timeSb.delete(timeSb.lastIndexOf("|"), timeSb.length());
                            }
                            if (positionSb.lastIndexOf("|") != -1) {
                                positionSb.delete(positionSb.lastIndexOf("|"), positionSb.length());
                            }
                            break;
                        }
                        j--;
                        currentOne = kwSynonymsPositions.get(j - 1)[currentIndexs[j - 1]];
                        currentLen = allSynonyms.get(j - 1)[currentIndexs[j - 1]].length();
                        currentIndexs[j]++;
                        j--; // 和for 循环中的j++相抵
                        //退出该层的元素
                        if (timeSb.lastIndexOf(",") != -1) {
                            timeSb.delete(timeSb.lastIndexOf(","), timeSb.length());
                        }
                        if (positionSb.lastIndexOf(",") != -1) {
                            positionSb.delete(positionSb.lastIndexOf(","), positionSb.length());
                        }
                    }
                }
                timeSb.append("|");
                positionSb.append("|");
            }
        }

        if (! ruleResultMap.containsKey(expAlias)) {
            ruleResultMap.put(expAlias, false);
        }

        for (String temp : timeSb.toString().split("\\|")) {
            if (temp.split(",").length == tempKeywords.length) {
                allPositions.add(temp);
            }
        }
        eachFactorSize.put(expAlias, allPositions.size());

        JSONObject eachFactorJo = new JSONObject();
        eachFactorJo.put(exp, allPositions);
        factorResultInfo.add(eachFactorJo);
        factorResultInfoAll.add(eachFactorJo);
        JSONObject eachWordInExpJo2 = new JSONObject();
        Iterator iterator = eachWordInExpJo.keys();
        while(iterator.hasNext()){
            String key = (String) iterator.next();
            eachWordInExpJo2.put(key,eachWordInExpJo.getString(key));
        }
        mergeKeywordTimes(factorResultInfoAll, eachWordInExpJo2);
        //添加结果到因子时间结果中
        if(!Constants.EACH_WORD_INFO.equals("unkeep")){
            mergeKeywordTimes(factorResultInfo, eachWordInExpJo);
        }else{
            mergeKeywordTimes(factorResultInfo, new JSONObject());
        }

    }

    private boolean canContinue(StringBuffer sb1, StringBuffer sb2, int len) {
        Set<Integer> ins = new HashSet<>();
        String[] s1 = sb2.toString().split("\\|");
        String[] s3 = sb1.toString().split("\\|");
        String[] s2 = s1[s1.length - 1].split(",");
        if (s1.length == 1) return true; //记下当前;
        else {

            for (int i = 0; i < s1.length - 1; i++) {
                if (s1[i].split(",").length < len) continue;
                for (int j = 0; j < s1[i].split(",").length; j++) {
                    ins.add(Integer.valueOf(s1[i].split(",")[j]));
                }
            }

            //循环已经之前找到的所有组合
            for (int i = 0; i < s1.length - 1; i++) {
                if (s1[i].split(",").length < len) continue;
                //如果当前正在运算的位置之前已经运算过，则判断过滤掉其中一个
                if (Integer.valueOf(s1[i].split(",")[s2.length - 1]) == Integer.valueOf(s2[s2.length - 1])) {
                    //如果之前找到的命中点更接近，则保留前一个
                    if (Integer.valueOf(s1[i].split(",")[s2.length - 2]) > Integer.valueOf(s2[s2.length - 2])) {
                        sb1.delete(sb1.lastIndexOf(","), sb1.length());
                        sb2.delete(sb2.lastIndexOf(","), sb2.length());
                        return false; //丢掉当前，continue;
                    }
                    else {
                        //记下当前;
                        sb1.delete(sb1.indexOf(s3[i]), sb1.indexOf(s3[i]) + s3[i].length() + 1);
                        sb2.delete(sb2.indexOf(s1[i]), sb2.indexOf(s1[i]) + s1[i].length() + 1);
                        return true;
                    }
                }

                for (String tmp : s2) {
                    if (ins.contains(Integer.valueOf(tmp))) {
                        sb1.delete(sb1.lastIndexOf(","), sb1.length());
                        sb2.delete(sb2.lastIndexOf(","), sb2.length());
                        return false;
                    }
                }

            }
        }
        ins.clear();
        return true;
    }

    private void mergeKeywordTimes(JSONArray factorResultInfo, JSONObject eachWordInExpJo) {
        Iterator<String> it;
        String factor;
        for (Object eachFactor : factorResultInfo) {
            JSONObject eachFactorJo = JSONObject.fromObject(eachFactor);
            it = eachFactorJo.keys();
            while(it.hasNext()) {
                factor = it.next();
                if (eachWordInExpJo.containsKey(factor)) {
                    eachWordInExpJo.remove(factor);
                }
            }
        }
        it = eachWordInExpJo.keys();
        while (it.hasNext()) {
            factor = it.next();
            JSONObject newFactor = new JSONObject();
            newFactor.put(factor, eachWordInExpJo.get(factor));
            factorResultInfo.add(newFactor);
        }
    }


    protected int doEachRule(String ruleAlias, String ruleExpression,
                             JSONObject kwsCache,
                             JSONArray ruleResultInfo, Object[] others, String... type) {
        Object[] newOthers;
        if (others != null && others[1] != null) {
            newOthers = new Object[4];
//            logger.info("ruleAlias : " + ruleAlias + ", time time time : " + others[1].toString());
            Integer t1, t2;
            t1 = Integer.valueOf(others[1].toString().split("-")[0].toString()) *
                    Integer.valueOf(smartv.getDuration()) / 100;
            t2 = Integer.valueOf(others[1].toString().split("-")[1].toString()) *
                    Integer.valueOf(smartv.getDuration()) / 100;

            newOthers[0] = others[0];
            newOthers[1] = t1 + "-" + t2;
            newOthers[2] = others[2];
            newOthers[3] = others[3];
        }
        else newOthers = others;

        Map<String, Boolean> ruleResultMap = new HashMap<>();
        JSONObject eachRuleJo = new JSONObject();
        List<Variable> variables = new ArrayList<>();
        JSONArray factorResultInfo = new JSONArray();
        JSONArray factorResultInfoAll = new JSONArray();
        eachRuleJo.put("ruleName", ((CommonQualityParam)param).getRuleNameMap().get(ruleAlias));
        eachRuleJo.put("ruleAlias", ruleAlias); // 例如 R1, R2, R3
        eachRuleJo.put("ruleExpressionParam", ruleExpression.replaceAll("Param"," "));
        if(experssionRuleMap.containsKey(ruleAlias)){
            eachRuleJo.put("ruleExpression", ((CommonQualityParam) param).getRuleMap().get(ruleAlias));
        }else{
            eachRuleJo.put("ruleExpression", ruleExpression);
        }
        Map<String, Object> result = ExpressionHandler.getFactorsFromExp(ruleExpression);
        String newRuleExpression = result.get("exp").toString();
        Map<String, String> expMap = (LinkedHashMap<String, String>) result.get("expMap");
        Map<String, String> wordMap = (LinkedHashMap<String, String>) result.get("wordMap");
        Map<String, String> bunchMap = (LinkedHashMap<String, String>) result.get("bunchMap");
        Map<String, String> exceptMap = (LinkedHashMap<String, String>) result.get("exceptMap");
//        Object qualityCache = cache.get("qualityCache");
//        JSONObject kwsCache = null;
//        if (type.length > 0) {
//            kwsCache = JSONObject.fromObject(qualityCache);
//        }
//        else {
//            if (! StringHelper.isNull(qualityCache)) {
//                if (qualityCache.toString().startsWith("V2")) {
//                    kwsCache = QualityUtil.trans2Json(qualityCache.toString());
//                }
//                else {
//                    qualityCache = qualityCache.toString().replaceAll("_", "");
//                    kwsCache = JSONObject.fromObject(qualityCache);
//                }
//            }
//            else kwsCache = JSONObject.fromObject(qualityCache);
//        }

        Map<String, Integer> eachFactorSize = new HashMap<>();
        for (Map.Entry<String, String> entry : wordMap.entrySet()) {
            doEachWord(entry.getKey(), entry.getValue(), kwsCache, eachFactorSize,
                    factorResultInfo,factorResultInfoAll, ruleResultMap, newOthers, type);
            variables.add(Variable.createVariable(entry.getKey(), true));
        }
        //根据cache处理near after before因子
        for (Map.Entry<String, String> entry : expMap.entrySet()) {
            doEachExp(entry.getKey(), entry.getValue(), kwsCache, eachFactorSize,
                    factorResultInfo,factorResultInfoAll, ruleResultMap, newOthers);
            variables.add(Variable.createVariable(entry.getKey(), true));
        }
        //处理连写因子
        for (Map.Entry<String, String> entry : bunchMap.entrySet()) {
            doEachBunchV2(entry.getKey(), entry.getValue(), kwsCache, eachFactorSize,
                    factorResultInfo,factorResultInfoAll, ruleResultMap, newOthers);
            variables.add(Variable.createVariable(entry.getKey(), true));
        }
        // 处理except
        for (Map.Entry<String, String> entry : exceptMap.entrySet()) {
            doEachExcept2(entry.getKey(), entry.getValue(), kwsCache, eachFactorSize,
                    factorResultInfo,factorResultInfoAll, ruleResultMap, newOthers);
            variables.add(Variable.createVariable(entry.getKey(), true));
        }
        boolean expResult =  ExpressionHandler.getValue(newRuleExpression, ruleResultMap);
        eachRuleJo.put("expResult", expResult);
        qualityResultMap.put(ruleAlias, expResult);
        JSONArray newJa = new JSONArray();
        JSONObject tmpJobj1, tmpJobj2;
        List<String> keywords = new ArrayList<>();
        String tmpKey;
        Iterator it;
        for (Object o : factorResultInfo) {
            tmpJobj1 = JSONObject.fromObject(o);
            it = tmpJobj1.keys();
            while (it.hasNext()) {
                tmpKey = it.next().toString();
                tmpJobj2 = new JSONObject();
                tmpJobj2.put("keyword", tmpKey);
                tmpJobj2.put("times", tmpJobj1.get(tmpKey));
                tmpJobj2.put("count", JSONArray.fromObject(tmpJobj1.get(tmpKey)).size());

                if (!keywords.contains(tmpKey) && tmpJobj2.getInt("count") > 0){
                    newJa.add(tmpJobj2);
                    keywords.add(tmpKey);
                }
            }

        }
        eachRuleJo.put("keywordinfo", newJa);
        keywords.clear();
        JSONArray newAllJa = new JSONArray();
        for (Object o : factorResultInfoAll) {
            tmpJobj1 = JSONObject.fromObject(o);
            it = tmpJobj1.keys();
            while (it.hasNext()) {
                tmpKey = it.next().toString();
                tmpJobj2 = new JSONObject();
                tmpJobj2.put("keyword", tmpKey);
                tmpJobj2.put("times", tmpJobj1.get(tmpKey));
                tmpJobj2.put("count", JSONArray.fromObject(tmpJobj1.get(tmpKey)).size());

                if (!keywords.contains(tmpKey) && tmpJobj2.getInt("count") > 0){
                    newAllJa.add(tmpJobj2);
                    keywords.add(tmpKey);
                }
            }

        }
        eachRuleJo.put("keywordinfoall", newAllJa);

        //计算规则的命中次数

        if (expResult) {
            String stackSet = ExpressionEvaluator.compile(newRuleExpression, variables);
            Stack<Object> stack = new Stack<>();
            int t1, t2;
            Object tmp;
            for (String s : stackSet.split(" ")) {
                if (s.equals("OR")) {
                    tmp = stack.pop();
                    if (StringUtils.isNumeric(tmp.toString())) t1 = Integer.valueOf(tmp.toString());
                    else t1 = eachFactorSize.get(tmp.toString());
                    tmp = stack.pop();
                    if (StringUtils.isNumeric(tmp.toString())) t2 = Integer.valueOf(tmp.toString());
                    else t2 = eachFactorSize.get(tmp.toString());
                    stack.push(t1 + t2);
                }
                else if (s.equals("AND")) {
                    tmp = stack.pop();
                    if (StringUtils.isNumeric(tmp.toString())) t1 = Integer.valueOf(tmp.toString());
                    else t1 = eachFactorSize.get(tmp.toString());
                    tmp = stack.pop();
                    if (StringUtils.isNumeric(tmp.toString())) t2 = Integer.valueOf(tmp.toString());
                    else t2 = eachFactorSize.get(tmp.toString());

                    if (t1 <= t2) stack.push(t1);
                    else stack.push(t2);
                }
                else if (s.equals("NOT")) {
                    tmp = stack.pop();
                    if (StringUtils.isNumeric(tmp.toString())) t1 = Integer.valueOf(tmp.toString());
                    else t1 = eachFactorSize.get(tmp.toString());
                    if (t1 == 0) stack.push(1);
                    else stack.push(0);
                }
                else stack.push(s);
            }
            if (stack.size() == 1) {
                tmp = stack.pop();
                if (StringUtils.isNumeric(tmp.toString())) {
                    eachRuleJo.put("riskCount", Integer.valueOf(tmp.toString()));
                }
                else if (eachFactorSize.containsKey(tmp)) {
                    eachRuleJo.put("riskCount", eachFactorSize.get(tmp));
                }
                else {
                    eachRuleJo.put("riskCount", 0);
                }
            }
        }
        else {
            eachRuleJo.put("riskCount", 0);
        }
//        logger.info("ruleAlias : " + ruleAlias + "; riskCount : " + eachRuleJo.get("riskCount"));
//        if (others != null) {
//            if (others[2] != null) {
//                logger.info(" rule count : " + others[2].toString());
//            }
//        }
        //计算最终命中
        if (others != null && others[2] != null) {
            if (Integer.valueOf(eachRuleJo.get("riskCount").toString()) >= Integer.valueOf(others[2].toString())) {
                eachRuleJo.put("expResult", true);
                qualityResultMap.put(ruleAlias, true);
            }
            else {
                eachRuleJo.put("expResult", false);
                qualityResultMap.put(ruleAlias, false);
            }
        }

        ruleResultInfo.add(eachRuleJo);
        return qualityResultMap.get(ruleAlias) ? 1 : 0;

    }

    protected void doCommonRule(CommonQualityParam param) {
        Map<String, String> commonRule = param.getCommonRule();
        if (commonRule.containsKey("longTalk")) {
            if (Double.valueOf(smartv.getDuration().trim()) >
                    Double.valueOf(commonRule.get("longTalk").trim())) {
                qualityResult.setLongTalk(true);
            }
        }

        if (commonRule.containsKey("muteInfo")) {
            int count = 0;
            if (! StringHelper.isNull(smartv.getMuteInfo())) {
                if (smartv.getMuteInfo().startsWith("[")) {
                    JSONArray muteJa = JSONArray.fromObject(smartv.getMuteInfo());
                    for (Iterator it = muteJa.iterator(); it.hasNext();) {
                        if (Double.valueOf(JSONObject.fromObject(it.next()).getString("blankLen"))
                                > Double.valueOf(commonRule.get("muteInfo").toString())) {
                            count++;
                        }
                    }
                }
            }
            qualityResult.setMuteRiskCount(count);
        }
        if (commonRule.containsKey("speedInfo")) {
            int count = 0;
            String text = (StringHelper.isNull(smartv.getSpeedResultA()) ? "" : smartv.getSpeedResultA())
                    + (StringHelper.isNull(smartv.getSpeedResultB()) ? "" : smartv.getSpeedResultB());
            String[] rows = text.split(";");
            for (String str : rows) {
                String[] elements = str.split(" ");
                if (3 > elements.length) continue;
                if (Double.valueOf(elements[2].toString()) * 60 > Double.valueOf(commonRule.get("speedInfo"))) {
                    count++;
                }
            }
            qualityResult.setSpeedRiskCount(count);
//            logger.info("speedInfo count : " + count + ", fileName : " + smartv.getFileName());
        }
        if (commonRule.containsKey("interruptInfo")) {
            int count = 0;
            double start, end;
            String text = (StringHelper.isNull(smartv.getRepInterrupt()) ? "" : smartv.getRepInterrupt())
                    + (StringHelper.isNull(smartv.getAccountInterrupt()) ? "" : smartv.getAccountInterrupt());
            String[] rows = text.split(";");
            for(String str : rows) {
                String[] elements = str.split("-");
                if(2 > elements.length) continue;
                try{
                    start = Double.parseDouble(elements[0]);
                    end  = Double.parseDouble(elements[1]);
                }catch(Exception e) {
                    continue;
                }
                if((end-start) > Double.valueOf(commonRule.get("interruptInfo"))) {
                    count++;
                }
            }
            qualityResult.setInterruptRiskCount(count);
        }

        if (commonRule.containsKey("qualityHs")) {
            JSONArray ja = JSONArray.fromObject(commonRule.get("qualityHs"));
            JSONObject tmpObj;
            String text;
            int count = 0;
            StringBuffer sb = new StringBuffer();
            double startTime, endTime;
            for (Object obj : ja) {
                count = 0;
                tmpObj = JSONObject.fromObject(obj);
                startTime = Integer.valueOf(smartv.getDuration()) * Integer.valueOf(tmpObj.get("timeFrom").toString()) / 100;
                endTime = Integer.valueOf(smartv.getDuration()) * Integer.valueOf(tmpObj.get("timeEnd").toString()) / 100;
                if ("A".equals(tmpObj.get("role"))) {
                    text = StringHelper.isNull(smartv.getSpeedResultA()) ? "" : smartv.getSpeedResultA();
                }
                else if ("B".equals(tmpObj.get("role"))) {
                    text = StringHelper.isNull(smartv.getSpeedResultB()) ? "" : smartv.getSpeedResultB();
                }
                else {
                    text = (StringHelper.isNull(smartv.getSpeedResultA()) ? "" : smartv.getSpeedResultA())
                            + (StringHelper.isNull(smartv.getSpeedResultB()) ? "" : smartv.getSpeedResultB());
                }

                String[] rows = text.split(";");
                for (String str : rows) {
                    String[] elements = str.split(" ");
                    if (3 > elements.length) continue;
//                    logger.info("elements 0 : " + elements[0]);
                    Double ld_double=Double.parseDouble(elements[0]);
                    if (Double.parseDouble(elements[0])>=startTime&&ld_double.intValue()<=endTime) {
                        if (Double.valueOf(elements[2].toString()) * 60 > Double.valueOf(tmpObj.get("zs").toString())) {
                            count++;
                        }
                    }
                }
                sb.append(",");
                sb.append(count);
            }
            if (ja.size() > 0) qualityResult.setQualityHs(sb.substring(1));

            if (smartv.getFileName().indexOf("#") == -1) {
//                logger.info("QualityHs count " + sb.substring(1) + ", filename : " + smartv.getFileName());
                qualityResult.setFileName(smartv.getFileName());
            }

        }
    }

    private String getNewFactor(String factor, String keyword1, String newKeyword1,
                                String keyword2, String newKeyword2) {
        String[] tempKeywords = factor.split(
                Constants.EXPRESSION_OPERATOR_AFTER +
                        "|" +  Constants.EXPRESSION_OPERATOR_BEFORE +
                        "|" +  Constants.EXPRESSION_OPERATOR_NEAR +
                        "|" + Constants.EXPRESSION_OPERATOR_EXCEPT
        );
        String kw1WithRole = tempKeywords[0];
        String kw2WithRole = tempKeywords[1];

        String splitStr = factor.replaceFirst(kw1WithRole, "")
                .replaceFirst(kw2WithRole, "")
                .replaceAll("\\(", "")
                .replaceAll("\\)", "");

        return (kw1WithRole.replaceFirst(keyword1, newKeyword1) + splitStr
                + kw2WithRole.replaceFirst(keyword2, newKeyword2))
                .replaceAll("\\(", "")
                .replaceAll("\\)", "");
    }

    // 处理 规则参数用索引值替换后 产生的多个值问题
    private String doExperssionParamAsSynonyms(String keywordBase,List<String> kw1s){
        if (keywordBase.contains("param")){
            String[] dealParams = keywordBase.split("param");
            for (int i=0;i<dealParams.length;i++) {
                if(i == 0){
                    keywordBase = dealParams[i];
                    continue;
                }
                if (i == dealParams.length-1){
                    continue;
                }
                kw1s.add(dealParams[i]);
            }
        }
        return keywordBase;
    }

    // 将exp 中的参数值用参数表达式替换
    private String doExperssionParamForExp(String keywordBase,String exp){
        if (keywordBase.contains("param")){
            String[] params =  keywordBase.split("param");
            String newExp = params[params.length-1];
            if(experssionParamMap.containsKey(newExp)){
                exp = exp.replace(experssionParamMap.get(newExp).replaceAll("Param","param"),"$${"+newExp+"}");
            }
        }
        return exp;
    }

    // 处理except
    private void doEachExcept(String expAlias, String exp,
                              JSONObject kwsCache,
                              Map<String, Integer> eachFactorSize,
                              JSONArray factorResultInfo,
                              Map<String, Boolean> ruleResultMap,
                              Object[] others) {

        String newFactor;
        String keyword1, keyword2;
        String synonyms1, synonyms2;
        KeyWordCache kwc1, kwc2;
        List<String> kw1s = new ArrayList<>(), kw2s = new ArrayList<>();
        List<String> allPositions = new ArrayList<>();
        JSONObject eachFactorJo = new JSONObject();
        JSONObject eachWordInExpJo = new JSONObject();
        int number = 0;
        String[] tempKeywords = exp.split(Constants.EXPRESSION_OPERATOR_EXCEPT);
        String kw1WithRole = tempKeywords[0].replaceAll("\\(", "").replaceAll("\\)", "");
        String kw2WithRole = tempKeywords[1].replaceAll("\\(", "").replaceAll("\\)", "");
        newFactor = exp;
        if (others != null && others[0] != null) {
            newFactor = exp.replaceFirst(kw1WithRole, "").replaceFirst(kw2WithRole, "");
            kw1WithRole = kw1WithRole.split("-").length > 1 ?
                    others[0].toString().toLowerCase().trim() + "-" + kw1WithRole.split("-")[1] :
                    others[0].toString().toLowerCase().trim() + "-" + kw1WithRole;
            if (kw1WithRole.startsWith("-")) kw1WithRole = kw1WithRole.substring(1);

            kw2WithRole = kw2WithRole.split("-").length > 1 ?
                    others[0].toString().toLowerCase().trim() + "-" + kw2WithRole.split("-")[1] :
                    others[0].toString().toLowerCase().trim() + "-" + kw2WithRole;
            if (kw2WithRole.startsWith("-")) kw2WithRole = kw2WithRole.substring(1);
            newFactor = kw1WithRole + newFactor + kw2WithRole;
        }

        if (kw1WithRole.split("-").length > 1) keyword1 = kw1WithRole.split("-")[1];
        else keyword1 = kw1WithRole;
        if (kw2WithRole.split("-").length > 1) keyword2 = kw2WithRole.split("-")[1];
        else keyword2 = kw2WithRole;

        Set<Integer> ins = new HashSet<>();
        if (kwsCache.containsKey(keyword1)) {
            kwc1 = (KeyWordCache) JSONObject.toBean(
                    JSONObject.fromObject(kwsCache.get(keyword1)), KeyWordCache.class);
        } else {
            kwc1 = smartv.makeCache(keyword1);
        }
        if (!eachWordInExpJo.containsKey(kw1WithRole)) {
            eachWordInExpJo.put(kw1WithRole, kwc1.getWordInExpTimesV2(kw1WithRole, others));
        }

        if (kwsCache.containsKey(keyword2)) {
            kwc2 = (KeyWordCache) JSONObject.toBean(
                    JSONObject.fromObject(kwsCache.get(keyword2)), KeyWordCache.class);
        } else {
            kwc2 = smartv.makeCache(keyword2);
        }
        if (!eachWordInExpJo.containsKey(kw2WithRole)) {
            eachWordInExpJo.put(kw2WithRole, kwc2.getWordInExpTimesV2(kw2WithRole, others));
        }
        allPositions.addAll(kwc1.getExceptFactorTimes(newFactor, kwc2, others, ins));
        if (kwc1.getExceptFactorValue(newFactor, kwc2, others)) {
            ruleResultMap.put(expAlias, true);
        }
        ins.clear();
        if (! ruleResultMap.containsKey(expAlias)) {
            ruleResultMap.put(expAlias, false);
        }
        eachFactorSize.put(expAlias, allPositions.size());
        eachFactorJo.put(exp, allPositions);
        factorResultInfo.add(eachFactorJo);
        //添加结果到因子时间结果中
        mergeKeywordTimes(factorResultInfo, eachWordInExpJo);
    }

    public void doEachExcept2(String expAlias, String exp,
                              JSONObject kwsCache,
                              Map<String, Integer> eachFactorSize,
                              JSONArray factorResultInfo,JSONArray factorResultInfoAll,
                              Map<String, Boolean> ruleResultMap,
                              Object[] others) {

        String newFactor;
        String keyword1, keyword2;
        String synonyms1, synonyms2;
        KeyWordCache kwc1, kwc2;
        List<String> kw1s = new ArrayList<>(), kw2s = new ArrayList<>();
        List<String> allPositions = new ArrayList<>();
        JSONObject eachFactorJo = new JSONObject();
        JSONObject eachWordInExpJo = new JSONObject();
        int number = 0;
        String[] tempKeywords = exp.split(Constants.EXPRESSION_OPERATOR_EXCEPT);
        String kw1WithRole = tempKeywords[0].replaceAll("\\(", "").replaceAll("\\)", "");
        String kw2WithRole = tempKeywords[1].replaceAll("\\(", "").replaceAll("\\)", "");
        newFactor = exp;
        if (others != null && others[0] != null) {
            newFactor = exp.replaceFirst(kw1WithRole, "").replaceFirst(kw2WithRole, "");
            kw1WithRole = kw1WithRole.split("-").length > 1 ?
                    others[0].toString().toLowerCase().trim() + "-" + kw1WithRole.split("-")[1] :
                    others[0].toString().toLowerCase().trim() + "-" + kw1WithRole;
            if (kw1WithRole.startsWith("-")) kw1WithRole = kw1WithRole.substring(1);

            kw2WithRole = kw2WithRole.split("-").length > 1 ?
                    others[0].toString().toLowerCase().trim() + "-" + kw2WithRole.split("-")[1] :
                    others[0].toString().toLowerCase().trim() + "-" + kw2WithRole;
            if (kw2WithRole.startsWith("-")) kw2WithRole = kw2WithRole.substring(1);
            newFactor = kw1WithRole + newFactor + kw2WithRole;
        }

        exp = doExperssionParamForExp(kw1WithRole,exp);
        exp = doExperssionParamForExp(kw2WithRole,exp);
        // 处理带参数的规则表达式用索引值替换后 出现多个值的问题
        kw1WithRole = doExperssionParamAsSynonyms(kw1WithRole,kw1s);
        kw2WithRole = doExperssionParamAsSynonyms(kw2WithRole,kw2s);

        if (kw1WithRole.split("-").length > 1) keyword1 = kw1WithRole.split("-")[1];
        else keyword1 = kw1WithRole;
        if (kw2WithRole.split("-").length > 1) keyword2 = kw2WithRole.split("-")[1];
        else keyword2 = kw2WithRole;

        Set<Integer> ins = new HashSet<>();
        // 同义词
        synonyms1 = RedisCacheService.initialize().getValueByKey("quality_basic:keyword", keyword1);
        for (String kw : kw1s){
            synonyms1 = synonyms1 + " " + kw;
        }
        kw1s.clear();
        kw1s.add(keyword1);
        if (! StringHelper.isNull(synonyms1)) kw1s.addAll(Arrays.asList(synonyms1.split(" ")));

        synonyms2 = RedisCacheService.initialize().getValueByKey("quality_basic:keyword", keyword2);
        for (String kw : kw2s){
            synonyms2 = synonyms2 + " " + kw;
        }
        kw2s.clear();
        kw2s.add(keyword2);
        if (! StringHelper.isNull(synonyms2)) kw2s.addAll(Arrays.asList(synonyms2.split(" ")));

        for (String kw1 : kw1s) {
            if (StringHelper.isNull(kw1)) continue;
            if (kwsCache.containsKey(kw1)) {
                number++; //用于判断第二个关键词组是第几次循环
                kwc1 = (KeyWordCache) JSONObject.toBean(
                        JSONObject.fromObject(kwsCache.get(kw1)), KeyWordCache.class);
                //保留全部表达式中的单个关键词的命中信息start
                if (keyword1.equals(kw1)) {
                    if (! eachWordInExpJo.containsKey(kw1WithRole)) {
                        eachWordInExpJo.put(kw1WithRole, kwc1.getWordInExpTimesV2(kw1WithRole, others));
                    }
                } else {
                    if (! eachWordInExpJo.containsKey(kw1WithRole + "[" + kw1WithRole.replaceAll(keyword1, kw1) + "]")) {
                        eachWordInExpJo.put(kw1WithRole + "[" + kw1WithRole.replaceAll(keyword1, kw1) + "]",
                                kwc1.getWordInExpTimesV2(kw1WithRole.replaceAll(keyword1, kw1), others));
                    }
                }
                for (String kw2 : kw2s) {
                    if (StringHelper.isNull(kw2)) continue;
                    if (kwsCache.containsKey(kw2)) {
                        kwc2 = (KeyWordCache) JSONObject.toBean(
                                JSONObject.fromObject(kwsCache.get(kw2)), KeyWordCache.class);
                        //保留全部表达式中的单个关键词的命中信息start
                        if (number == 1) {
                            if (keyword2.equals(kw2)) {
                                if (!eachWordInExpJo.containsKey(kw2WithRole)) {
                                    eachWordInExpJo.put(kw2WithRole, kwc2.getWordInExpTimesV2(kw2WithRole, others));
                                }
                            } else {
                                if (!eachWordInExpJo.containsKey(kw2WithRole + "[" + kw2WithRole.replaceAll(keyword2, kw2) + "]")) {
                                    eachWordInExpJo.put(kw2WithRole + "[" + kw2WithRole.replaceAll(keyword2, kw2) + "]",
                                            kwc2.getWordInExpTimesV2(kw2WithRole.replaceAll(keyword2, kw2), others));
                                }
                            }
                        }

                        if(kw2.contains(kw1)){
                            // 使用当前同义词重组表达式
                            newFactor = getNewFactor(newFactor, keyword1, kw1, keyword2, kw2);
                            // 计算当前重组后的表达式的命中时间点
                            allPositions.addAll(kwc1.getExceptFactorTimes(newFactor, kwc2, others, ins));
                            if (kwc1.getExceptFactorValue(newFactor, kwc2, others)) {
                                ruleResultMap.put(expAlias, true);
                            }
                        }

                    }
                }

            }
        }
        ins.clear();
        number = 0;
        if (! ruleResultMap.containsKey(expAlias)) {
            //keyword1为新增词 或 keyword2为新增词
            if (! kwsCache.containsKey(keyword1) || ! kwsCache.containsKey(keyword2)) {
                for (String kw1 : kw1s) {
                    if (StringHelper.isNull(kw1)) continue;
                    number++;
                    if (kwsCache.containsKey(kw1)) {
                        kwc1 = (KeyWordCache) JSONObject.toBean(
                                JSONObject.fromObject(kwsCache.get(kw1)), KeyWordCache.class);
                    } else {
                        kwc1 = smartv.makeCache(kw1);
                    }

                    //保留全部表达式中的单个关键词的命中信息start
                    if (keyword1.equals(kw1)) {
                        if (!eachWordInExpJo.containsKey(kw1WithRole)) {
                            eachWordInExpJo.put(kw1WithRole, kwc1.getWordInExpTimesV2(kw1WithRole, others));
                        }
                    } else {
                        if (!eachWordInExpJo.containsKey(kw1WithRole + "[" + kw1WithRole.replaceAll(keyword1, kw1) + "]")) {
                            eachWordInExpJo.put(kw1WithRole + "[" + kw1WithRole.replaceAll(keyword1, kw1) + "]",
                                    kwc1.getWordInExpTimesV2(kw1WithRole.replaceAll(keyword1, kw1), others));
                        }
                    }
                    //保留全部表达式中的单个关键词的命中信息end

                    for (String kw2 : kw2s) {
                        if (StringHelper.isNull(kw2)) continue;
                        if (kwsCache.containsKey(kw2)) {
                            kwc2 = (KeyWordCache) JSONObject.toBean(
                                    JSONObject.fromObject(kwsCache.get(kw2)), KeyWordCache.class);
                        } else {
                            kwc2 = smartv.makeCache(kw2);
                        }
                        //保留全部表达式中的单个关键词的命中信息start
                        if (number == 1) {
                            if (keyword2.equals(kw2)) {
                                if (!eachWordInExpJo.containsKey(kw2WithRole)) {
                                    eachWordInExpJo.put(kw2WithRole, kwc2.getWordInExpTimesV2(kw2WithRole, others));
                                }
                            } else {
                                if (!eachWordInExpJo.containsKey(kw2WithRole + "[" + kw2WithRole.replaceAll(keyword2, kw2) + "]")) {
                                    eachWordInExpJo.put(kw2WithRole + "[" + kw2WithRole.replaceAll(keyword2, kw2) + "]",
                                            kwc2.getWordInExpTimesV2(kw2WithRole.replaceAll(keyword2, kw2), others));
                                }
                            }
                        }
                        //保留全部表达式中的单个关键词的命中信息end
                        if(kw2.contains(kw1)){
                            newFactor = getNewFactor(newFactor, keyword1, kw1, keyword2, kw2);
                            allPositions.addAll(kwc1.getExceptFactorTimes(newFactor, kwc2, others, ins));
                            if (kwc1.getExceptFactorValue(newFactor, kwc2, others)) {
                                ruleResultMap.put(expAlias, true);
                            }
                        }
                    }
                }
            }
        }
        ins.clear();
        if (! ruleResultMap.containsKey(expAlias)) {
            ruleResultMap.put(expAlias, false);
        }
        eachFactorSize.put(expAlias, allPositions.size());
        eachFactorJo.put(exp, allPositions);
        factorResultInfo.add(eachFactorJo);
        factorResultInfoAll.add(eachFactorJo);
        //添加结果到因子时间结果中
        JSONObject eachWordInExpJo2 = new JSONObject();
        Iterator iterator = eachWordInExpJo.keys();
        while(iterator.hasNext()){
            String key = (String) iterator.next();
            eachWordInExpJo2.put(key,eachWordInExpJo.getString(key));
        }
        mergeKeywordTimes(factorResultInfoAll, eachWordInExpJo2);
        if(!Constants.EACH_WORD_INFO.equals("unkeep")){
            mergeKeywordTimes(factorResultInfo, eachWordInExpJo);
        }else{
            mergeKeywordTimes(factorResultInfo, new JSONObject());
        }
    }
    private static void writeErrorLog(String method,String content,Exception e){
        BaseSystemOutLog.writeLog(BaseSystemOutLog.LogType_Error, "QualityRoutineThread", method, content, e);
    }
    private static void writeSystemLog(String method,String content){
        BaseSystemOutLog.writeLog(BaseSystemOutLog.LogType_Info, "QualityRoutineThread", method, content);
    }
}