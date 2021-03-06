/*
*  Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
*  Unless required by applicable law or agreed to in writing,
*  software distributed under the License is distributed on an
*  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
*  KIND, either express or implied.  See the License for the
*  specific language governing permissions and limitations
*  under the License.
*/
package org.ballerinalang.util.semantics;

import org.ballerinalang.model.Action;
import org.ballerinalang.model.ActionSymbolName;
import org.ballerinalang.model.AnnotationAttachment;
import org.ballerinalang.model.AnnotationAttachmentPoint;
import org.ballerinalang.model.AnnotationAttributeDef;
import org.ballerinalang.model.AnnotationAttributeValue;
import org.ballerinalang.model.AnnotationDef;
import org.ballerinalang.model.AttachmentPoint;
import org.ballerinalang.model.BLangPackage;
import org.ballerinalang.model.BLangProgram;
import org.ballerinalang.model.BTypeMapper;
import org.ballerinalang.model.BallerinaAction;
import org.ballerinalang.model.BallerinaConnectorDef;
import org.ballerinalang.model.BallerinaFile;
import org.ballerinalang.model.BallerinaFunction;
import org.ballerinalang.model.CallableUnit;
import org.ballerinalang.model.CallableUnitSymbolName;
import org.ballerinalang.model.CompilationUnit;
import org.ballerinalang.model.ConstDef;
import org.ballerinalang.model.ExecutableMultiReturnExpr;
import org.ballerinalang.model.Function;
import org.ballerinalang.model.FunctionSymbolName;
import org.ballerinalang.model.GlobalVariableDef;
import org.ballerinalang.model.Identifier;
import org.ballerinalang.model.ImportPackage;
import org.ballerinalang.model.NamespaceDeclaration;
import org.ballerinalang.model.NamespaceSymbolName;
import org.ballerinalang.model.NativeUnit;
import org.ballerinalang.model.NodeLocation;
import org.ballerinalang.model.NodeVisitor;
import org.ballerinalang.model.Operator;
import org.ballerinalang.model.ParameterDef;
import org.ballerinalang.model.Resource;
import org.ballerinalang.model.Service;
import org.ballerinalang.model.SimpleVariableDef;
import org.ballerinalang.model.StructDef;
import org.ballerinalang.model.SymbolName;
import org.ballerinalang.model.SymbolScope;
import org.ballerinalang.model.VariableDef;
import org.ballerinalang.model.Worker;
import org.ballerinalang.model.expressions.AbstractExpression;
import org.ballerinalang.model.expressions.ActionInvocationExpr;
import org.ballerinalang.model.expressions.AddExpression;
import org.ballerinalang.model.expressions.AndExpression;
import org.ballerinalang.model.expressions.ArrayInitExpr;
import org.ballerinalang.model.expressions.BasicLiteral;
import org.ballerinalang.model.expressions.BinaryArithmeticExpression;
import org.ballerinalang.model.expressions.BinaryExpression;
import org.ballerinalang.model.expressions.BinaryLogicalExpression;
import org.ballerinalang.model.expressions.CallableUnitInvocationExpr;
import org.ballerinalang.model.expressions.ConnectorInitExpr;
import org.ballerinalang.model.expressions.DivideExpr;
import org.ballerinalang.model.expressions.EqualExpression;
import org.ballerinalang.model.expressions.Expression;
import org.ballerinalang.model.expressions.FunctionInvocationExpr;
import org.ballerinalang.model.expressions.GreaterEqualExpression;
import org.ballerinalang.model.expressions.GreaterThanExpression;
import org.ballerinalang.model.expressions.InstanceCreationExpr;
import org.ballerinalang.model.expressions.JSONArrayInitExpr;
import org.ballerinalang.model.expressions.JSONInitExpr;
import org.ballerinalang.model.expressions.KeyValueExpr;
import org.ballerinalang.model.expressions.LambdaExpression;
import org.ballerinalang.model.expressions.LessEqualExpression;
import org.ballerinalang.model.expressions.LessThanExpression;
import org.ballerinalang.model.expressions.MapInitExpr;
import org.ballerinalang.model.expressions.ModExpression;
import org.ballerinalang.model.expressions.MultExpression;
import org.ballerinalang.model.expressions.NotEqualExpression;
import org.ballerinalang.model.expressions.NullLiteral;
import org.ballerinalang.model.expressions.OrExpression;
import org.ballerinalang.model.expressions.RefTypeInitExpr;
import org.ballerinalang.model.expressions.StringTemplateLiteral;
import org.ballerinalang.model.expressions.StructInitExpr;
import org.ballerinalang.model.expressions.SubtractExpression;
import org.ballerinalang.model.expressions.TypeCastExpression;
import org.ballerinalang.model.expressions.TypeConversionExpr;
import org.ballerinalang.model.expressions.UnaryExpression;
import org.ballerinalang.model.expressions.XMLCommentLiteral;
import org.ballerinalang.model.expressions.XMLElementLiteral;
import org.ballerinalang.model.expressions.XMLLiteral;
import org.ballerinalang.model.expressions.XMLPILiteral;
import org.ballerinalang.model.expressions.XMLQNameExpr;
import org.ballerinalang.model.expressions.XMLSequenceLiteral;
import org.ballerinalang.model.expressions.XMLTextLiteral;
import org.ballerinalang.model.expressions.variablerefs.FieldBasedVarRefExpr;
import org.ballerinalang.model.expressions.variablerefs.IndexBasedVarRefExpr;
import org.ballerinalang.model.expressions.variablerefs.SimpleVarRefExpr;
import org.ballerinalang.model.expressions.variablerefs.VariableReferenceExpr;
import org.ballerinalang.model.expressions.variablerefs.XMLAttributesRefExpr;
import org.ballerinalang.model.statements.AbortStmt;
import org.ballerinalang.model.statements.ActionInvocationStmt;
import org.ballerinalang.model.statements.AssignStmt;
import org.ballerinalang.model.statements.BlockStmt;
import org.ballerinalang.model.statements.BreakStmt;
import org.ballerinalang.model.statements.CommentStmt;
import org.ballerinalang.model.statements.ContinueStmt;
import org.ballerinalang.model.statements.ForkJoinStmt;
import org.ballerinalang.model.statements.FunctionInvocationStmt;
import org.ballerinalang.model.statements.IfElseStmt;
import org.ballerinalang.model.statements.NamespaceDeclarationStmt;
import org.ballerinalang.model.statements.ReplyStmt;
import org.ballerinalang.model.statements.RetryStmt;
import org.ballerinalang.model.statements.ReturnStmt;
import org.ballerinalang.model.statements.Statement;
import org.ballerinalang.model.statements.StatementKind;
import org.ballerinalang.model.statements.ThrowStmt;
import org.ballerinalang.model.statements.TransactionStmt;
import org.ballerinalang.model.statements.TransformStmt;
import org.ballerinalang.model.statements.TryCatchStmt;
import org.ballerinalang.model.statements.VariableDefStmt;
import org.ballerinalang.model.statements.WhileStmt;
import org.ballerinalang.model.statements.WorkerInvocationStmt;
import org.ballerinalang.model.statements.WorkerReplyStmt;
import org.ballerinalang.model.symbols.BLangSymbol;
import org.ballerinalang.model.types.BArrayType;
import org.ballerinalang.model.types.BFunctionType;
import org.ballerinalang.model.types.BJSONConstraintType;
import org.ballerinalang.model.types.BMapType;
import org.ballerinalang.model.types.BType;
import org.ballerinalang.model.types.BTypes;
import org.ballerinalang.model.types.SimpleTypeName;
import org.ballerinalang.model.types.TypeConstants;
import org.ballerinalang.model.types.TypeEdge;
import org.ballerinalang.model.types.TypeLattice;
import org.ballerinalang.model.types.TypeTags;
import org.ballerinalang.model.util.LangModelUtils;
import org.ballerinalang.model.values.BFloat;
import org.ballerinalang.model.values.BString;
import org.ballerinalang.natives.NativeUnitProxy;
import org.ballerinalang.natives.connectors.AbstractNativeAction;
import org.ballerinalang.runtime.worker.WorkerDataChannel;
import org.ballerinalang.util.codegen.InstructionCodes;
import org.ballerinalang.util.exceptions.BLangExceptionHelper;
import org.ballerinalang.util.exceptions.LinkerException;
import org.ballerinalang.util.exceptions.SemanticErrors;
import org.ballerinalang.util.exceptions.SemanticException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;

import javax.xml.XMLConstants;

import static org.ballerinalang.util.BLangConstants.INIT_FUNCTION_SUFFIX;

/**
 * {@code SemanticAnalyzer} analyzes semantic properties of a Ballerina program.
 *
 * @since 0.8.0
 */
public class SemanticAnalyzer implements NodeVisitor {
    private static final String ERRORS_PACKAGE = "ballerina.lang.errors";
    private static final String BALLERINA_CAST_ERROR = "TypeCastError";
    private static final String BALLERINA_CONVERSION_ERROR = "TypeConversionError";
    private static final String BALLERINA_ERROR = "Error";

    private String currentPkg;
    private CallableUnit currentCallableUnit = null;
    private Stack<CallableUnit> parentCallableUnit = new Stack<>();

    private Stack<SymbolScope> parentScope = new Stack<>();

    private int whileStmtCount = 0;
    private int transactionStmtCount = 0;
    private int failedBlockCount = 0;
    private boolean isWithinWorker = false;
    private SymbolScope currentScope;
    private SymbolScope currentPackageScope;
    private SymbolScope nativeScope;

    private BlockStmt.BlockStmtBuilder pkgInitFuncStmtBuilder;

    public SemanticAnalyzer(BLangProgram programScope) {
        currentScope = programScope;
        this.nativeScope = programScope.getNativeScope();
    }

    @Override
    public void visit(BLangProgram bLangProgram) {
        BLangPackage entryPkg = bLangProgram.getEntryPackage();
        if (entryPkg != null) {
            entryPkg.accept(this);
        } else {
            BLangPackage[] blangPackages = bLangProgram.getLibraryPackages();
            for (BLangPackage bLangPackage : blangPackages) {
                bLangPackage.accept(this);
            }
        }
    }

    @Override
    public void visit(BLangPackage bLangPackage) {
        BLangPackage[] dependentPackages = bLangPackage.getDependentPackages();
        List<BallerinaFunction> initFunctionList = new ArrayList<>();
        for (BLangPackage dependentPkg : dependentPackages) {
            if (dependentPkg.isSymbolsDefined()) {
                continue;
            }

            dependentPkg.accept(this);
            initFunctionList.add(dependentPkg.getInitFunction());
        }

        currentScope = bLangPackage;
        currentPackageScope = currentScope;
        currentPkg = bLangPackage.getPackagePath();

        // Create package.<init> function
        NodeLocation pkgLocation = bLangPackage.getNodeLocation();
        if (pkgLocation == null) {
            BallerinaFile[] ballerinaFiles = bLangPackage.getBallerinaFiles();

            // TODO filename becomes "" for built-in packages. FIX ME
            String filename = ballerinaFiles.length == 0 ? "" :
                    ballerinaFiles[0].getFileName();
            pkgLocation = new NodeLocation("", filename, 0);
        }

        BallerinaFunction.BallerinaFunctionBuilder functionBuilder =
                new BallerinaFunction.BallerinaFunctionBuilder(bLangPackage);
        functionBuilder.setNodeLocation(pkgLocation);
        functionBuilder.setIdentifier(new Identifier(bLangPackage.getPackagePath() + INIT_FUNCTION_SUFFIX));
        functionBuilder.setPkgPath(bLangPackage.getPackagePath());
        pkgInitFuncStmtBuilder = new BlockStmt.BlockStmtBuilder(bLangPackage.getNodeLocation(),
                bLangPackage);

        // Invoke <init> methods of all the dependent packages
        addDependentPkgInitCalls(initFunctionList, pkgInitFuncStmtBuilder, pkgLocation);

        // Define package level constructs
        defineStructs(bLangPackage.getStructDefs());
        defineConnectors(bLangPackage.getConnectors());
        resolveStructFieldTypes(bLangPackage.getStructDefs());
        defineFunctions(bLangPackage.getFunctions());
        defineServices(bLangPackage.getServices());
        defineAnnotations(bLangPackage.getAnnotationDefs());

        for (CompilationUnit compilationUnit : bLangPackage.getCompilationUnits()) {
            compilationUnit.accept(this);
        }

        // Complete the package init function
        ReturnStmt returnStmt = new ReturnStmt(pkgLocation, null, new Expression[0]);
        pkgInitFuncStmtBuilder.addStmt(returnStmt);
        pkgInitFuncStmtBuilder.setBlockKind(StatementKind.CALLABLE_UNIT_BLOCK);
        functionBuilder.setBody(pkgInitFuncStmtBuilder.build());
        BallerinaFunction initFunction = functionBuilder.buildFunction();
        initFunction.setReturnParamTypes(new BType[0]);
        bLangPackage.setInitFunction(initFunction);

        bLangPackage.setSymbolsDefined(true);
    }

    @Override
    public void visit(BallerinaFile bFile) {
    }

    @Override
    public void visit(ImportPackage importPkg) {
    }

    @Override
    public void visit(ConstDef constDef) {
        VariableDefStmt variableDefStmt = constDef.getVariableDefStmt();
        variableDefStmt.getVariableDef().setKind(VariableDef.Kind.CONSTANT);
        variableDefStmt.accept(this);

        for (AnnotationAttachment annotationAttachment : constDef.getAnnotations()) {
            annotationAttachment.setAttachedPoint(new AnnotationAttachmentPoint(AttachmentPoint.CONSTANT, null));
            annotationAttachment.accept(this);
        }

        // Insert constant initialization stmt to the package init function
        SimpleVarRefExpr varRefExpr = new SimpleVarRefExpr(constDef.getNodeLocation(),
                constDef.getWhiteSpaceDescriptor(), constDef.getName(), null, null);
        varRefExpr.setVariableDef(constDef);
        AssignStmt assignStmt = new AssignStmt(constDef.getNodeLocation(),
                new Expression[]{varRefExpr}, variableDefStmt.getRExpr());
        pkgInitFuncStmtBuilder.addStmt(assignStmt);
    }

    @Override
    public void visit(GlobalVariableDef globalVarDef) {
        VariableDefStmt variableDefStmt = globalVarDef.getVariableDefStmt();
        variableDefStmt.getVariableDef().setKind(VariableDef.Kind.GLOBAL_VAR);
        variableDefStmt.accept(this);

        if (variableDefStmt.getRExpr() != null) {
            // Create an assignment statement
            // Insert global variable initialization stmt to the package init function
            AssignStmt assignStmt = new AssignStmt(variableDefStmt.getNodeLocation(),
                    new Expression[]{variableDefStmt.getLExpr()}, variableDefStmt.getRExpr());
            pkgInitFuncStmtBuilder.addStmt(assignStmt);
        }
    }

    @Override
    public void visit(Service service) {
        // Visit the contents within a service
        // Open a new symbol scope
        openScope(service);

        for (AnnotationAttachment annotationAttachment : service.getAnnotations()) {
            annotationAttachment.setAttachedPoint(new AnnotationAttachmentPoint(AttachmentPoint.SERVICE,
                    service.getProtocolPkgPath()));
            annotationAttachment.accept(this);
        }

        //TODO if this validation is present, then can't run main methods in a file which has a service
//        if (!DispatcherRegistry.getInstance().protocolPkgExist(service.getProtocolPkgPath())) {
//            throw BLangExceptionHelper.getSemanticError(service.getNodeLocation(),
//                    SemanticErrors.INVALID_SERVICE_PROTOCOL, service.getProtocolPkgPath());
//        }

        for (VariableDefStmt variableDefStmt : service.getVariableDefStmts()) {
            variableDefStmt.getVariableDef().setKind(VariableDef.Kind.SERVICE_VAR);
            variableDefStmt.accept(this);
        }

        createServiceInitFunction(service);

        // Visit the set of resources in a service
        for (Resource resource : service.getResources()) {
            resource.accept(this);
        }

        // Close the symbol scope
        closeScope();
    }

    @Override
    public void visit(BallerinaConnectorDef connectorDef) {
        // Open the connector namespace
        openScope(connectorDef);

        if (connectorDef.isFilterConnector()) {
            BType type = BTypes.resolveType(connectorDef.getFilterSupportedType(),
                    currentScope, connectorDef.getNodeLocation());
            if (type != null) {
                if (type instanceof BallerinaConnectorDef) {
                    connectorDef.setFilteredType(type);
                    BallerinaConnectorDef filterConnector = (BallerinaConnectorDef) type;
                    if (!filterConnector.equals(connectorDef)) {
                        BLangExceptionHelper.throwSemanticError(connectorDef,
                                SemanticErrors.CONNECTOR_TYPES_NOT_EQUIVALENT,
                                connectorDef.getName(), filterConnector.getName());
                    }
                } else {
                    BLangExceptionHelper.throwSemanticError(connectorDef,
                            SemanticErrors.FILTER_CONNECTOR_MUST_BE_A_CONNECTOR,
                            type.getName());
                }
            } else {
                BLangExceptionHelper.throwSemanticError(connectorDef,
                        SemanticErrors.UNDEFINED_CONNECTOR,
                        connectorDef.getFilterSupportedType());
            }
        }

        for (AnnotationAttachment annotationAttachment : connectorDef.getAnnotations()) {
            annotationAttachment.setAttachedPoint(new AnnotationAttachmentPoint(AttachmentPoint.CONNECTOR, null));
            annotationAttachment.accept(this);
        }

        for (ParameterDef parameterDef : connectorDef.getParameterDefs()) {
            parameterDef.setKind(VariableDef.Kind.CONNECTOR_VAR);
            parameterDef.accept(this);
        }

        for (VariableDefStmt variableDefStmt : connectorDef.getVariableDefStmts()) {
            variableDefStmt.getVariableDef().setKind(VariableDef.Kind.CONNECTOR_VAR);
            variableDefStmt.accept(this);
        }

        createConnectorInitFunction(connectorDef);

        for (BallerinaAction action : connectorDef.getActions()) {
            action.accept(this);
        }

        // Close the symbol scope
        closeScope();
    }

    @Override
    public void visit(Resource resource) {
        // Visit the contents within a resource
        // Open a new symbol scope
        openScope(resource);
        currentCallableUnit = resource;

        // TODO Check whether the reply statement is missing. Ignore if the function does not return anything.
        //checkForMissingReplyStmt(resource);

        for (AnnotationAttachment annotationAttachment : resource.getAnnotations()) {
            annotationAttachment.setAttachedPoint(new AnnotationAttachmentPoint(AttachmentPoint.RESOURCE, null));
            annotationAttachment.accept(this);
        }

        for (ParameterDef parameterDef : resource.getParameterDefs()) {
            parameterDef.setKind(VariableDef.Kind.LOCAL_VAR);
            parameterDef.accept(this);
        }

        for (Worker worker : resource.getWorkers()) {
            visit(worker);
        }

        BlockStmt blockStmt = resource.getResourceBody();
        blockStmt.accept(this);
        checkAndAddReplyStmt(blockStmt);

        resolveWorkerInteractions(resource);

        // Close the symbol scope
        currentCallableUnit = null;
        closeScope();
    }

    private void buildWorkerInteractions(CallableUnit callableUnit, Worker[] workers, boolean isWorkerInWorker,
                                         boolean isForkJoinStmt) {
        // This map holds the worker data channels against the respective source and target workers
        Map<String, WorkerDataChannel> workerDataChannels = new HashMap<>();
        boolean statementCompleted = false;
        List<Statement> processedStatements = new ArrayList<>();

        if (callableUnit.getWorkerInteractionStatements() != null &&
                !callableUnit.getWorkerInteractionStatements().isEmpty()) {
            String sourceWorkerName;
            String targetWorkerName;
            for (Statement statement : callableUnit.getWorkerInteractionStatements()) {
                statementCompleted = false;
                if (statement instanceof WorkerInvocationStmt) {
                    targetWorkerName = ((WorkerInvocationStmt) statement).getName();
                    if (targetWorkerName == "fork" && isForkJoinStmt) {
                        break;
                    }
                    if (callableUnit instanceof Worker) {
                        sourceWorkerName = callableUnit.getName();
                    } else {
                        sourceWorkerName = "default";
                    }
                    // Find a matching worker reply statment
                    for (Worker worker : workers) {
                        if (statementCompleted) {
                            break;
                        }
                        Statement[] workerInteractions = worker.getWorkerInteractionStatements().
                                toArray(new Statement[worker.getWorkerInteractionStatements().size()]);
                        for (Statement workerInteraction : workerInteractions) {
                            if (workerInteraction instanceof WorkerReplyStmt) {
                                String complimentSourceWorkerName = ((WorkerReplyStmt) workerInteraction).
                                        getWorkerName();
                                String complimentTargetWorkerName = worker.getName();
                                if (sourceWorkerName.equals(complimentSourceWorkerName)
                                        && targetWorkerName.equals(complimentTargetWorkerName)) {
                                    // Statements are matching for their names. Check the parameters
                                    // Check for number of variables send and received
                                    Expression[] invokeParams = ((WorkerInvocationStmt) statement).getExpressionList();
                                    Expression[] receiveParams = ((WorkerReplyStmt) workerInteraction).
                                            getExpressionList();
                                    if (invokeParams.length != receiveParams.length) {
                                        break;
                                    } else {
                                        int i = 0;
                                        for (Expression invokeParam : invokeParams) {
                                            if (!(receiveParams[i++].getType().equals(invokeParam.getType()))) {
                                                break;
                                            }
                                        }
                                    }
                                    // Nothing wrong with the statements. Now create the data channel
                                    // and pop the statement.
                                    String interactionName = sourceWorkerName + "->" + targetWorkerName;
                                    WorkerDataChannel workerDataChannel;
                                    if (!workerDataChannels.containsKey(interactionName)) {
                                        workerDataChannel = new
                                                WorkerDataChannel(sourceWorkerName, targetWorkerName);
                                        workerDataChannels.put(interactionName, workerDataChannel);
                                    } else {
                                        workerDataChannel = workerDataChannels.get(interactionName);
                                    }

                                    ((WorkerInvocationStmt) statement).setWorkerDataChannel(workerDataChannel);
                                    ((WorkerReplyStmt) workerInteraction).
                                            setWorkerDataChannel(workerDataChannel);
                                    ((WorkerReplyStmt) workerInteraction).
                                            setEnclosingCallableUnitName(callableUnit.getName());
                                    callableUnit.addWorkerDataChannel(workerDataChannel);
                                    ((WorkerInvocationStmt) statement).setEnclosingCallableUnitName(
                                            callableUnit.getName());
                                    ((WorkerInvocationStmt) statement).setPackagePath(callableUnit.getPackagePath());
                                    worker.getWorkerInteractionStatements().remove(workerInteraction);
                                    processedStatements.add(statement);
                                    statementCompleted = true;
                                    break;
                                }
                            }
                        }
                    }
                } else {
                    sourceWorkerName = ((WorkerReplyStmt) statement).getWorkerName();
                    if (callableUnit instanceof Worker) {
                        targetWorkerName = callableUnit.getName();
                    } else {
                        targetWorkerName = "default";
                    }
                    // Find a matching worker invocation statment
                    for (Worker worker : callableUnit.getWorkers()) {
                        if (statementCompleted) {
                            break;
                        }
                        Statement[] workerInteractions = worker.getWorkerInteractionStatements().
                                toArray(new Statement[worker.getWorkerInteractionStatements().size()]);
                        for (Statement workerInteraction : workerInteractions) {
                            if (workerInteraction instanceof WorkerInvocationStmt) {
                                String complimentTargetWorkerName = ((WorkerInvocationStmt) workerInteraction).
                                        getName();
                                String complimentSourceWorkerName = worker.getName();
                                if (sourceWorkerName.equals(complimentSourceWorkerName) &&
                                        targetWorkerName.equals(complimentTargetWorkerName)) {
                                    // Statements are matching for their names. Check the parameters
                                    // Check for number of variables send and received
                                    Expression[] invokeParams = ((WorkerReplyStmt) statement).getExpressionList();
                                    Expression[] receiveParams = ((WorkerInvocationStmt) workerInteraction).
                                            getExpressionList();
                                    if (invokeParams.length != receiveParams.length) {
                                        break;
                                    } else {
                                        int i = 0;
                                        for (Expression invokeParam : invokeParams) {
                                            if (!(receiveParams[i++].getType().equals(invokeParam.getType()))) {
                                                break;
                                            }
                                        }
                                    }
                                    // Nothing wrong with the statements. Now create the data channel and
                                    // pop the statement.
                                    String interactionName = sourceWorkerName + "->" + targetWorkerName;
                                    WorkerDataChannel workerDataChannel;
                                    if (!workerDataChannels.containsKey(interactionName)) {
                                        workerDataChannel = new
                                                WorkerDataChannel(sourceWorkerName, targetWorkerName);
                                        workerDataChannels.put(interactionName, workerDataChannel);
                                    } else {
                                        workerDataChannel = workerDataChannels.get(interactionName);
                                    }

                                    ((WorkerReplyStmt) statement).setWorkerDataChannel(workerDataChannel);
                                    ((WorkerInvocationStmt) workerInteraction).
                                            setWorkerDataChannel(workerDataChannel);
                                    ((WorkerInvocationStmt) workerInteraction).
                                            setEnclosingCallableUnitName(callableUnit.getName());
                                    callableUnit.addWorkerDataChannel(workerDataChannel);
                                    ((WorkerReplyStmt) statement).setEnclosingCallableUnitName(callableUnit.getName());
                                    ((WorkerReplyStmt) statement).setPackagePath(callableUnit.getPackagePath());
                                    worker.getWorkerInteractionStatements().remove(workerInteraction);
                                    processedStatements.add(statement);
                                    statementCompleted = true;
                                    break;
                                }
                            }
                        }
                    }
                }

                if (!statementCompleted && !isWorkerInWorker) {
                    // TODO: Need to have a specific error message
                    BLangExceptionHelper.throwSemanticError(statement,
                            SemanticErrors.WORKER_INTERACTION_NOT_VALID);
                }
            }
            callableUnit.getWorkerInteractionStatements().removeAll(processedStatements);
        }
    }

    private void resolveWorkerInteractions(CallableUnit callableUnit) {
        //CallableUnit callableUnit = function;
        boolean isWorkerInWorker = callableUnit instanceof Worker;
        boolean isForkJoinStmt = callableUnit instanceof ForkJoinStmt;
        Worker[] workers = callableUnit.getWorkers();
        if (workers.length > 0) {
            Worker[] tempWorkers = new Worker[workers.length];
            System.arraycopy(workers, 0, tempWorkers, 0, tempWorkers.length);
            int i = 0;
            do {
                buildWorkerInteractions(callableUnit, tempWorkers, isWorkerInWorker, isForkJoinStmt);
                callableUnit = workers[i];
                i++;
                System.arraycopy(workers, i, tempWorkers, 0, workers.length - i);
            } while (i < workers.length);
        }
    }

    private void resolveForkJoin(ForkJoinStmt forkJoinStmt) {
        Worker[] workers = forkJoinStmt.getWorkers();
        if (workers != null && workers.length > 0) {
            for (Worker worker : workers) {
                for (Statement statement : worker.getWorkerInteractionStatements()) {
                    if (statement instanceof WorkerInvocationStmt) {
                        String targetWorkerName = ((WorkerInvocationStmt) statement).getName();
                        if (targetWorkerName.equalsIgnoreCase("fork")) {
                            String sourceWorkerName = worker.getName();
                            WorkerDataChannel workerDataChannel = new WorkerDataChannel
                                    (sourceWorkerName, targetWorkerName);
                            ((WorkerInvocationStmt) statement).setWorkerDataChannel(workerDataChannel);
                            currentCallableUnit.addWorkerDataChannel(workerDataChannel);
                        }
                    }
                }
            }
        }
    }

    @Override
    public void visit(BallerinaFunction function) {
        // Open a new symbol scope
        openScope(function);
        currentCallableUnit = function;

        for (AnnotationAttachment annotationAttachment : function.getAnnotations()) {
            annotationAttachment.setAttachedPoint(new AnnotationAttachmentPoint(AttachmentPoint.FUNCTION, null));
            annotationAttachment.accept(this);
        }

        for (ParameterDef parameterDef : function.getParameterDefs()) {
            parameterDef.setKind(VariableDef.Kind.LOCAL_VAR);
            parameterDef.accept(this);
        }

        for (ParameterDef parameterDef : function.getReturnParameters()) {
            // Check whether these are unnamed set of return types.
            // If so break the loop. You can't have a mix of unnamed and named returns parameters.
            if (parameterDef.getName() != null) {
                parameterDef.setKind(VariableDef.Kind.LOCAL_VAR);
            }

            parameterDef.accept(this);
        }

        if (!function.isNative()) {
            for (Worker worker : function.getWorkers()) {
                worker.accept(this);
            }

            BlockStmt blockStmt = function.getCallableUnitBody();
            blockStmt.accept(this);
            checkAndAddReturnStmt(function);
        }

        resolveWorkerInteractions(function);

        // Close the symbol scope
        currentCallableUnit = null;
        closeScope();
    }

    @Override
    public void visit(BTypeMapper typeMapper) {
    }

    @Override
    public void visit(BallerinaAction action) {
        // Open a new symbol scope
        openScope(action);
        currentCallableUnit = action;

        for (AnnotationAttachment annotationAttachment : action.getAnnotations()) {
            annotationAttachment.setAttachedPoint(new AnnotationAttachmentPoint(AttachmentPoint.ACTION, null));
            annotationAttachment.accept(this);
        }

        for (ParameterDef parameterDef : action.getParameterDefs()) {
            parameterDef.setKind(VariableDef.Kind.LOCAL_VAR);
            parameterDef.accept(this);
        }

        for (ParameterDef parameterDef : action.getReturnParameters()) {
            // Check whether these are unnamed set of return types.
            // If so break the loop. You can't have a mix of unnamed and named returns parameters.
            if (parameterDef.getName() != null) {
                parameterDef.setKind(VariableDef.Kind.LOCAL_VAR);
            }

            parameterDef.accept(this);
        }

        if (!action.isNative()) {
            for (Worker worker : action.getWorkers()) {
                worker.accept(this);
            }

            BlockStmt blockStmt = action.getCallableUnitBody();
            blockStmt.accept(this);
            checkAndAddReturnStmt(action);
        }
        resolveWorkerInteractions(action);

        // Close the symbol scope
        currentCallableUnit = null;
        closeScope();
    }

    @Override
    public void visit(Worker worker) {
        // Open a new symbol scope. This is done manually to avoid falling back to package scope
        parentScope.push(currentScope);
        currentScope = worker;
        parentCallableUnit.push(currentCallableUnit);
        currentCallableUnit = worker;

        for (ParameterDef parameterDef : worker.getParameterDefs()) {
            parameterDef.setKind(VariableDef.Kind.LOCAL_VAR);
            parameterDef.accept(this);
        }

        for (ParameterDef parameterDef : worker.getReturnParameters()) {
            parameterDef.setKind(VariableDef.Kind.LOCAL_VAR);
            parameterDef.accept(this);
        }

        // Define the worker at symbol scope so that workers defined within this worker can invoke this
        // addWorkerSymbol(worker);

        for (Worker worker2 : worker.getWorkers()) {
            worker2.accept(this);
        }


        BlockStmt blockStmt = worker.getCallableUnitBody();
        isWithinWorker = true;
        blockStmt.accept(this);
        isWithinWorker = false;

        // Close the symbol scope
        currentCallableUnit = parentCallableUnit.pop();
        // Close symbol scope. This is done manually to avoid falling back to package scope
        currentScope = parentScope.pop();
    }

    private void addWorkerSymbol(Worker worker) {
        SymbolName symbolName = worker.getSymbolName();
        BLangSymbol varSymbol = currentScope.resolve(symbolName);
        if (varSymbol != null) {
            BLangExceptionHelper.throwSemanticError(worker,
                    SemanticErrors.REDECLARED_SYMBOL, worker.getName());
        }
        currentScope.define(symbolName, worker);
    }

    @Override
    public void visit(StructDef structDef) {
        for (AnnotationAttachment annotationAttachment : structDef.getAnnotations()) {
            annotationAttachment.setAttachedPoint(new AnnotationAttachmentPoint(AttachmentPoint.STRUCT, null));
            annotationAttachment.accept(this);
        }
    }

    @Override
    public void visit(AnnotationAttachment annotation) {
        AnnotationAttachmentPoint attachedPoint = annotation.getAttachedPoint();
        SymbolName annotationSymName = new SymbolName(annotation.getName(), annotation.getPkgPath());
        BLangSymbol annotationSymbol = currentScope.resolve(annotationSymName);

        if (!(annotationSymbol instanceof AnnotationDef)) {
            BLangExceptionHelper.throwSemanticError(annotation, SemanticErrors.UNDEFINED_ANNOTATION,
                    annotationSymName);
        }

        // Validate the attached point
        AnnotationDef annotationDef = (AnnotationDef) annotationSymbol;
        if (annotationDef.getAttachmentPoints() != null && annotationDef.getAttachmentPoints().length > 0) {
            Optional<AnnotationAttachmentPoint> matchingAttachmentPoint = Arrays
                    .stream(annotationDef.getAttachmentPoints())
                    .filter(attachmentPoint -> attachmentPoint.equals(attachedPoint))
                    .findAny();
            if (!matchingAttachmentPoint.isPresent()) {
                String msg = attachedPoint.getAttachmentPoint().getValue();
                if (attachedPoint.getPkgPath() != null) {
                    msg = attachedPoint.getAttachmentPoint().getValue() + "<" + attachedPoint.getPkgPath() + ">";
                }
                throw BLangExceptionHelper.getSemanticError(annotation.getNodeLocation(),
                        SemanticErrors.ANNOTATION_NOT_ALLOWED, annotationSymName, msg);
            }
        }

        // Validate the attributes and their types
        validateAttributes(annotation, annotationDef);

        // Populate default values for annotation attributes
        populateDefaultValues(annotation, annotationDef);
    }

    /**
     * Visit and validate attributes of an annotation attachment.
     *
     * @param annotation    Annotation attachment to validate attributes
     * @param annotationDef Definition of the annotation
     */
    private void validateAttributes(AnnotationAttachment annotation, AnnotationDef annotationDef) {
        annotation.getAttributeNameValuePairs().forEach((attributeName, attributeValue) -> {
            // Check attribute existence
            BLangSymbol attributeSymbol = annotationDef.resolveMembers(new SymbolName(attributeName));
            if (attributeSymbol == null || !(attributeSymbol instanceof AnnotationAttributeDef)) {
                BLangExceptionHelper.throwSemanticError(annotation, SemanticErrors.NO_SUCH_ATTRIBUTE,
                        attributeName, annotation.getName());
            }

            // Check types
            AnnotationAttributeDef attributeDef = ((AnnotationAttributeDef) attributeSymbol);
            SimpleTypeName attributeType = attributeDef.getTypeName();
            if (attributeValue.getVarRefExpr() != null) {
                SimpleVarRefExpr varRefExpr = attributeValue.getVarRefExpr();
                visitSingleValueExpr(varRefExpr);
                if (!(varRefExpr.getVariableDef() instanceof ConstDef)) {
                    throw BLangExceptionHelper.getSemanticError(attributeValue.getNodeLocation(),
                            SemanticErrors.ATTRIBUTE_VAL_CANNOT_REFER_NON_CONST);
                }
                attributeValue.setType(varRefExpr.getType());
                BType lhsType = BTypes.resolveType(attributeType, currentScope, annotation.getNodeLocation());
                if (lhsType != varRefExpr.getType()) {
                    throw BLangExceptionHelper.getSemanticError(attributeValue.getNodeLocation(),
                            SemanticErrors.INCOMPATIBLE_TYPES, lhsType, varRefExpr.getType());
                }
                return;
            }
            SimpleTypeName valueType = attributeValue.getTypeName();
            BLangSymbol valueTypeSymbol = currentScope.resolve(valueType.getSymbolName());
            BLangSymbol attributeTypeSymbol = annotationDef.resolve(new SymbolName(attributeType.getName(),
                    attributeType.getPackagePath()));

            if (attributeType.isArrayType()) {
                if (!valueType.isArrayType()) {
                    BLangExceptionHelper.throwSemanticError(attributeValue, SemanticErrors.INCOMPATIBLE_TYPES,
                            attributeTypeSymbol.getSymbolName() + TypeConstants.ARRAY_TNAME,
                            valueTypeSymbol.getSymbolName());
                }

                AnnotationAttributeValue[] valuesArray = attributeValue.getValueArray();
                for (AnnotationAttributeValue value : valuesArray) {
                    valueTypeSymbol = currentScope.resolve(value.getTypeName().getSymbolName());
                    if (attributeTypeSymbol != valueTypeSymbol) {
                        BLangExceptionHelper.throwSemanticError(attributeValue, SemanticErrors.INCOMPATIBLE_TYPES,
                                attributeTypeSymbol.getSymbolName(), valueTypeSymbol.getSymbolName());
                    }

                    // If the value of the attribute is another annotation, then recursively
                    // traverse to its attributes and validate
                    AnnotationAttachment childAnnotation = value.getAnnotationValue();
                    if (childAnnotation != null && valueTypeSymbol instanceof AnnotationDef) {
                        validateAttributes(childAnnotation, (AnnotationDef) valueTypeSymbol);
                    }
                }
            } else {
                if (valueType.isArrayType()) {
                    BLangExceptionHelper.throwSemanticError(attributeValue,
                            SemanticErrors.INCOMPATIBLE_TYPES_ARRAY_FOUND, attributeTypeSymbol.getName());
                }

                if (attributeTypeSymbol != valueTypeSymbol) {
                    BLangExceptionHelper.throwSemanticError(attributeValue, SemanticErrors.INCOMPATIBLE_TYPES,
                            attributeTypeSymbol.getSymbolName(), valueTypeSymbol.getSymbolName());
                }

                // If the value of the attribute is another annotation, then recursively
                // traverse to its attributes and validate
                AnnotationAttachment childAnnotation = attributeValue.getAnnotationValue();
                if (childAnnotation != null && valueTypeSymbol instanceof AnnotationDef) {
                    validateAttributes(childAnnotation, (AnnotationDef) valueTypeSymbol);
                }
            }
        });
    }

    /**
     * Populate default values to the annotation attributes.
     *
     * @param annotation    Annotation attachment to populate default values
     * @param annotationDef Definition of the annotation corresponds to the provided annotation attachment
     */
    private void populateDefaultValues(AnnotationAttachment annotation, AnnotationDef annotationDef) {
        Map<String, AnnotationAttributeValue> attributeValPairs = annotation.getAttributeNameValuePairs();
        for (AnnotationAttributeDef attributeDef : annotationDef.getAttributeDefs()) {
            String attributeName = attributeDef.getName();

            // if the current attribute is not defined in the annotation attachment, populate it with default value
            if (!attributeValPairs.containsKey(attributeName)) {
                BasicLiteral defaultValue = attributeDef.getAttributeValue();
                if (defaultValue != null) {
                    annotation.addAttributeNameValuePair(attributeName,
                            new AnnotationAttributeValue(defaultValue.getBValue(),
                                    defaultValue.getTypeName(), null, null));
                }
                continue;
            }

            // If the annotation attachment contains the current attribute, and if the value is another 
            // annotationAttachment, then recursively populate its default values
            AnnotationAttributeValue attributeValue = attributeValPairs.get(attributeName);
            if (attributeValue.getVarRefExpr() != null) {
                continue;
            }
            SimpleTypeName valueType = attributeValue.getTypeName();
            if (valueType.isArrayType()) {
                AnnotationAttributeValue[] valuesArray = attributeValue.getValueArray();
                for (AnnotationAttributeValue value : valuesArray) {
                    AnnotationAttachment annotationTypeVal = value.getAnnotationValue();

                    // skip if the array element is not an annotation
                    if (annotationTypeVal == null) {
                        continue;
                    }

                    SimpleTypeName attributeType = attributeDef.getTypeName();
                    BLangSymbol attributeTypeSymbol = annotationDef.resolve(
                            new SymbolName(attributeType.getName(), attributeType.getPackagePath()));
                    if (attributeTypeSymbol instanceof AnnotationDef) {
                        populateDefaultValues(annotationTypeVal, (AnnotationDef) attributeTypeSymbol);
                    }
                }
            } else {
                AnnotationAttachment annotationTypeVal = attributeValue.getAnnotationValue();

                // skip if the value is not an annotation
                if (annotationTypeVal == null) {
                    continue;
                }

                BLangSymbol attributeTypeSymbol = annotationDef.resolve(attributeDef.getTypeName().getSymbolName());
                if (attributeTypeSymbol instanceof AnnotationDef) {
                    populateDefaultValues(annotationTypeVal, (AnnotationDef) attributeTypeSymbol);
                }
            }
        }
    }

    @Override
    public void visit(AnnotationAttributeDef annotationAttributeDef) {
        SimpleTypeName fieldType = annotationAttributeDef.getTypeName();
        BasicLiteral fieldVal = annotationAttributeDef.getAttributeValue();

        if (fieldVal != null) {
            fieldVal.accept(this);
            BType valueType = fieldVal.getType();

            if (!BTypes.isBuiltInTypeName(fieldType.getName())) {
                BLangExceptionHelper.throwSemanticError(annotationAttributeDef, SemanticErrors.INVALID_DEFAULT_VALUE);
            }

            BLangSymbol typeSymbol = currentScope.resolve(fieldType.getSymbolName());
            BType fieldBType = (BType) typeSymbol;
            if (!BTypes.isValueType(fieldBType)) {
                BLangExceptionHelper.throwSemanticError(annotationAttributeDef, SemanticErrors.INVALID_DEFAULT_VALUE);
            }

            if (fieldBType != valueType) {
                BLangExceptionHelper.throwSemanticError(annotationAttributeDef,
                        SemanticErrors.INVALID_OPERATION_INCOMPATIBLE_TYPES, fieldType, fieldVal.getTypeName());
            }
        } else {
            BLangSymbol typeSymbol;
            if (fieldType.isArrayType()) {
                typeSymbol = currentScope.resolve(new SymbolName(fieldType.getName(), fieldType.getPackagePath()));
            } else {
                typeSymbol = currentScope.resolve(fieldType.getSymbolName());
            }

            // Check whether the field type is a built in value type or an annotation.
            if (((typeSymbol instanceof BType) && !BTypes.isValueType((BType) typeSymbol)) ||
                    (!(typeSymbol instanceof BType) && !(typeSymbol instanceof AnnotationDef))) {
                BLangExceptionHelper.throwSemanticError(annotationAttributeDef, SemanticErrors.INVALID_ATTRIBUTE_TYPE,
                        fieldType);
            }

            if (!(typeSymbol instanceof BType)) {
                fieldType.setPkgPath(annotationAttributeDef.getPackagePath());
            }
        }
    }

    @Override
    public void visit(AnnotationDef annotationDef) {
        for (AnnotationAttributeDef fields : annotationDef.getAttributeDefs()) {
            fields.accept(this);
        }

        for (AnnotationAttachment annotationAttachment : annotationDef.getAnnotations()) {
            annotationAttachment.setAttachedPoint(new AnnotationAttachmentPoint(AttachmentPoint.ANNOTATION, null));
            annotationAttachment.accept(this);
        }
    }

    @Override
    public void visit(ParameterDef paramDef) {
        BType bType = BTypes.resolveType(paramDef.getTypeName(), currentScope, paramDef.getNodeLocation());
        paramDef.setType(bType);

        if (paramDef.getAnnotations() == null) {
            return;
        }

        for (AnnotationAttachment annotationAttachment : paramDef.getAnnotations()) {
            annotationAttachment.setAttachedPoint(new AnnotationAttachmentPoint(AttachmentPoint.PARAMETER, null));
            annotationAttachment.accept(this);
        }
    }

    @Override
    public void visit(SimpleVariableDef varDef) {
    }


    // Visit statements

    @Override
    public void visit(VariableDefStmt varDefStmt) {
        // Resolves the type of the variable
        VariableDef varDef = varDefStmt.getVariableDef();
        BType lhsType = BTypes.resolveType(varDef.getTypeName(), currentScope, varDef.getNodeLocation());
        varDef.setType(lhsType);

        // Set the Variable kind if it is not set.
        if (varDef.getKind() == null) {
            // Here we assume that this is a local variable
            varDef.setKind(VariableDef.Kind.LOCAL_VAR);
        }

        // Mark the this variable references as LHS expressions
        ((VariableReferenceExpr) varDefStmt.getLExpr()).setLHSExpr(true);

        // Check whether this variable is already defined, if not define it.
        SymbolName symbolName = new SymbolName(varDef.getName(), currentPkg);
        BLangSymbol varSymbol = currentScope.resolve(symbolName);
        if (varSymbol != null && varSymbol.getSymbolScope().getScopeName() == currentScope.getScopeName()) {
            BLangExceptionHelper.throwSemanticError(varDef, SemanticErrors.REDECLARED_SYMBOL, varDef.getName());
        }
        currentScope.define(symbolName, varDef);

        Expression rExpr = varDefStmt.getRExpr();
        if (rExpr == null) {
            return;
        }

        if (rExpr instanceof RefTypeInitExpr) {
            RefTypeInitExpr refTypeInitExpr = getNestedInitExpr(rExpr, lhsType);
            varDefStmt.setRExpr(refTypeInitExpr);
            refTypeInitExpr.accept(this);
            return;
        }

        BType rhsType;
        if (rExpr instanceof ExecutableMultiReturnExpr) {
            rExpr.accept(this);
            ExecutableMultiReturnExpr multiReturnExpr = (ExecutableMultiReturnExpr) rExpr;
            BType[] returnTypes = multiReturnExpr.getTypes();

            if (returnTypes.length != 1) {
                BLangExceptionHelper.throwSemanticError(varDefStmt, SemanticErrors.ASSIGNMENT_COUNT_MISMATCH,
                        "1", returnTypes.length);
            }

            rhsType = returnTypes[0];
        } else {
            visitSingleValueExpr(rExpr);
            rhsType = rExpr.getType();
        }

        // Check whether the right-hand type can be assigned to the left-hand type.
        AssignabilityResult result = performAssignabilityCheck(lhsType, rExpr);
        if (result.expression != null) {
            varDefStmt.setRExpr(result.expression);
        } else if (!result.assignable) {
            BLangExceptionHelper.throwSemanticError(varDefStmt, SemanticErrors.INCOMPATIBLE_ASSIGNMENT,
                    rhsType, lhsType);
        }
    }

    @Override
    public void visit(AssignStmt assignStmt) {
        Expression[] lExprs = assignStmt.getLExprs();

        visitLExprsOfAssignment(assignStmt, lExprs);

        Expression rExpr = assignStmt.getRExpr();
        if (rExpr instanceof FunctionInvocationExpr || rExpr instanceof ActionInvocationExpr) {
            rExpr.accept(this);
            if (assignStmt.isDeclaredWithVar()) {
                assignVariableRefTypes(lExprs, ((CallableUnitInvocationExpr) rExpr).getTypes());
            }
            checkForMultiAssignmentErrors(assignStmt, lExprs, (CallableUnitInvocationExpr) rExpr);
            return;
        }

        if (lExprs.length > 1 && (rExpr instanceof TypeCastExpression || rExpr instanceof TypeConversionExpr)) {
            ((AbstractExpression) rExpr).setMultiReturnAvailable(true);
            rExpr.accept(this);
            if (assignStmt.isDeclaredWithVar()) {
                assignVariableRefTypes(lExprs, ((ExecutableMultiReturnExpr) rExpr).getTypes());
            }
            checkForMultiValuedCastingErrors(assignStmt, lExprs, (ExecutableMultiReturnExpr) rExpr);
            return;
        }

        // Now we know that this is a single value assignment statement.
        Expression lExpr = assignStmt.getLExprs()[0];
        BType lhsType = lExpr.getType();

        if (rExpr instanceof RefTypeInitExpr) {
            if (assignStmt.isDeclaredWithVar()) {
                BLangExceptionHelper.throwSemanticError(assignStmt, SemanticErrors.INVALID_VAR_ASSIGNMENT);
            }
            RefTypeInitExpr refTypeInitExpr = getNestedInitExpr(rExpr, lhsType);
            assignStmt.setRExpr(refTypeInitExpr);
            refTypeInitExpr.accept(this);
            return;
        }

        visitSingleValueExpr(rExpr);
        BType rhsType = rExpr.getType();
        if (assignStmt.isDeclaredWithVar()) {
            ((SimpleVarRefExpr) lExpr).getVariableDef().setType(rhsType);
            lhsType = rhsType;
        }

        // Check whether the right-hand type can be assigned to the left-hand type.
        AssignabilityResult result = performAssignabilityCheck(lhsType, rExpr);
        if (result.expression != null) {
            assignStmt.setRExpr(result.expression);
        } else if (!result.assignable) {
            BLangExceptionHelper.throwSemanticError(assignStmt, SemanticErrors.INCOMPATIBLE_ASSIGNMENT,
                    rhsType, lhsType);
        }
    }

    @Override
    public void visit(BlockStmt blockStmt) {
        openScope(blockStmt);

        for (int stmtIndex = 0; stmtIndex < blockStmt.getStatements().length; stmtIndex++) {
            Statement stmt = blockStmt.getStatements()[stmtIndex];
            if (stmt instanceof BreakStmt && whileStmtCount < 1) {
                BLangExceptionHelper.throwSemanticError(stmt,
                        SemanticErrors.BREAK_STMT_NOT_ALLOWED_HERE);
            }
            if (stmt instanceof ContinueStmt && whileStmtCount < 1) {
                BLangExceptionHelper.throwSemanticError(stmt,
                        SemanticErrors.CONTINUE_STMT_NOT_ALLOWED_HERE);
            }

            if (stmt instanceof AbortStmt && transactionStmtCount < 1) {
                BLangExceptionHelper.throwSemanticError(stmt,
                        SemanticErrors.ABORT_STMT_NOT_ALLOWED_HERE);
            }

            if (stmt instanceof RetryStmt && failedBlockCount < 1) {
                BLangExceptionHelper.throwSemanticError(stmt,
                        SemanticErrors.RETRY_STMT_NOT_ALLOWED_HERE);
            }


            if (isWithinWorker) {
                if (stmt instanceof ReplyStmt) {
                    BLangExceptionHelper.throwSemanticError(stmt,
                            SemanticErrors.REPLY_STMT_NOT_ALLOWED_HERE);
                }
//                else if (stmt instanceof ReturnStmt) {
//                    BLangExceptionHelper.throwSemanticError(stmt,
//                            SemanticErrors.RETURN_STMT_NOT_ALLOWED_HERE);
//                }
            }

            if (stmt instanceof BreakStmt || stmt instanceof ContinueStmt || stmt instanceof ReplyStmt ||
                    stmt instanceof AbortStmt || stmt instanceof RetryStmt) {
                checkUnreachableStmt(blockStmt.getStatements(), stmtIndex + 1);
            }

            stmt.accept(this);

            if (stmt.isAlwaysReturns()) {
                checkUnreachableStmt(blockStmt.getStatements(), stmtIndex + 1);
                blockStmt.setAlwaysReturns(true);
            }
        }

        closeScope();
    }

    @Override
    public void visit(CommentStmt commentStmt) {

    }

    @Override
    public void visit(IfElseStmt ifElseStmt) {
        boolean stmtReturns = true;
        Expression expr = ifElseStmt.getCondition();
        visitSingleValueExpr(expr);

        if (expr.getType() != BTypes.typeBoolean) {
            BLangExceptionHelper
                    .throwSemanticError(ifElseStmt, SemanticErrors.INCOMPATIBLE_TYPES_BOOLEAN_EXPECTED, expr.getType());
        }

        Statement thenBody = ifElseStmt.getThenBody();
        thenBody.accept(this);

        stmtReturns &= thenBody.isAlwaysReturns();

        for (IfElseStmt.ElseIfBlock elseIfBlock : ifElseStmt.getElseIfBlocks()) {
            Expression elseIfCondition = elseIfBlock.getElseIfCondition();
            visitSingleValueExpr(elseIfCondition);

            if (elseIfCondition.getType() != BTypes.typeBoolean) {
                BLangExceptionHelper.throwSemanticError(ifElseStmt, SemanticErrors.INCOMPATIBLE_TYPES_BOOLEAN_EXPECTED,
                        elseIfCondition.getType());
            }

            Statement elseIfBody = elseIfBlock.getElseIfBody();
            elseIfBody.accept(this);

            stmtReturns &= elseIfBody.isAlwaysReturns();
        }

        Statement elseBody = ifElseStmt.getElseBody();
        if (elseBody != null) {
            elseBody.accept(this);
            stmtReturns &= elseBody.isAlwaysReturns();
        } else {
            stmtReturns = false;
        }

        ifElseStmt.setAlwaysReturns(stmtReturns);
    }

    @Override
    public void visit(WhileStmt whileStmt) {
        whileStmtCount++;
        Expression expr = whileStmt.getCondition();
        visitSingleValueExpr(expr);

        if (expr.getType() != BTypes.typeBoolean) {
            BLangExceptionHelper
                    .throwSemanticError(whileStmt, SemanticErrors.INCOMPATIBLE_TYPES_BOOLEAN_EXPECTED, expr.getType());
        }

        BlockStmt blockStmt = whileStmt.getBody();
        if (blockStmt.getStatements().length == 0) {
            // This can be optimized later to skip the while statement
            BLangExceptionHelper.throwSemanticError(blockStmt, SemanticErrors.NO_STATEMENTS_WHILE_LOOP);
        }

        blockStmt.accept(this);
        whileStmtCount--;
    }

    @Override
    public void visit(BreakStmt breakStmt) {
        checkParent(breakStmt);
    }

    @Override
    public void visit(ContinueStmt continueStmt) {
        checkParent(continueStmt);
    }

    @Override
    public void visit(TryCatchStmt tryCatchStmt) {
        tryCatchStmt.getTryBlock().accept(this);

        BLangSymbol error = currentScope.resolve(new SymbolName(BALLERINA_ERROR, ERRORS_PACKAGE));
        Set<BType> definedTypes = new HashSet<>();
        if (tryCatchStmt.getCatchBlocks().length != 0) {
            // Assumption : To use CatchClause, ballerina.lang.errors should be resolved before.
            if (error == null || !(error instanceof StructDef)) {
                BLangExceptionHelper.throwSemanticError(tryCatchStmt,
                        SemanticErrors.CANNOT_RESOLVE_STRUCT, ERRORS_PACKAGE, BALLERINA_ERROR);
            }
        }
        for (TryCatchStmt.CatchBlock catchBlock : tryCatchStmt.getCatchBlocks()) {
            catchBlock.getParameterDef().setKind(VariableDef.Kind.LOCAL_VAR);
            catchBlock.getParameterDef().accept(this);
            // Validation for error type.
            if (!error.equals(catchBlock.getParameterDef().getType()) &&
                    (!(catchBlock.getParameterDef().getType() instanceof StructDef) ||
                            TypeLattice.getExplicitCastLattice().getEdgeFromTypes(catchBlock.getParameterDef()
                                    .getType(), error, null) == null)) {
                throw new SemanticException(BLangExceptionHelper.constructSemanticError(
                        catchBlock.getCatchBlockStmt().getNodeLocation(),
                        SemanticErrors.ONLY_ERROR_TYPE_ALLOWED_HERE));
            }
            // Validation for duplicate catch.
            if (!definedTypes.add(catchBlock.getParameterDef().getType())) {
                throw new SemanticException(BLangExceptionHelper.constructSemanticError(
                        catchBlock.getCatchBlockStmt().getNodeLocation(),
                        SemanticErrors.DUPLICATED_ERROR_CATCH, catchBlock.getParameterDef().getTypeName()));
            }
            catchBlock.getCatchBlockStmt().accept(this);
        }

        if (tryCatchStmt.getFinallyBlock() != null) {
            tryCatchStmt.getFinallyBlock().getFinallyBlockStmt().accept(this);
        }
    }

    @Override
    public void visit(ThrowStmt throwStmt) {
        throwStmt.getExpr().accept(this);
        BType expressionType = null;
        if (throwStmt.getExpr() instanceof SimpleVarRefExpr && throwStmt.getExpr().getType() instanceof StructDef) {
            expressionType = throwStmt.getExpr().getType();
        } else if (throwStmt.getExpr() instanceof FunctionInvocationExpr) {
            FunctionInvocationExpr funcIExpr = (FunctionInvocationExpr) throwStmt.getExpr();
            if (!funcIExpr.isMultiReturnExpr() && funcIExpr.getTypes().length == 1 && funcIExpr.getTypes()[0]
                    instanceof StructDef) {
                expressionType = funcIExpr.getTypes()[0];
            }
        }
        if (expressionType != null) {
            BLangSymbol error = currentScope.resolve(new SymbolName(BALLERINA_ERROR, ERRORS_PACKAGE));
            // TODO : Fix this.
            // Assumption : To use CatchClause, ballerina.lang.errors should be resolved before.
            if (error == null) {
                BLangExceptionHelper.throwSemanticError(throwStmt,
                        SemanticErrors.CANNOT_RESOLVE_STRUCT, ERRORS_PACKAGE, BALLERINA_ERROR);
            }
            if (error.equals(expressionType) || TypeLattice.getExplicitCastLattice().getEdgeFromTypes
                    (expressionType, error, null) != null) {
                throwStmt.setAlwaysReturns(true);
                return;
            }
        }
        throw new SemanticException(BLangExceptionHelper.constructSemanticError(
                throwStmt.getNodeLocation(), SemanticErrors.ONLY_ERROR_TYPE_ALLOWED_HERE));
    }

    @Override
    public void visit(FunctionInvocationStmt functionInvocationStmt) {
        functionInvocationStmt.getFunctionInvocationExpr().accept(this);
    }

    @Override
    public void visit(ActionInvocationStmt actionInvocationStmt) {
        actionInvocationStmt.getActionInvocationExpr().accept(this);
    }

    @Override
    public void visit(WorkerInvocationStmt workerInvocationStmt) {


        Expression[] expressions = workerInvocationStmt.getExpressionList();
        BType[] bTypes = new BType[expressions.length];
        int p = 0;
        for (Expression expression : expressions) {
            expression.accept(this);
            bTypes[p++] = expression.getType();
        }

        workerInvocationStmt.setTypes(bTypes);


        if (workerInvocationStmt.getCallableUnitName() != null &&
                !workerInvocationStmt.getCallableUnitName().equals("default") &&
                !workerInvocationStmt.getCallableUnitName().equals("fork")) {
            linkWorker(workerInvocationStmt);

            //Find the return types of this function invocation expression.
//            ParameterDef[] returnParams = workerInvocationStmt.getCallableUnit().getReturnParameters();
//            BType[] returnTypes = new BType[returnParams.length];
//            for (int i = 0; i < returnParams.length; i++) {
//                returnTypes[i] = returnParams[i].getTypeName();
//            }
//            workerInvocationStmt.setTypes(returnTypes);
        }
    }

    @Override
    public void visit(WorkerReplyStmt workerReplyStmt) {
        String workerName = workerReplyStmt.getWorkerName();
        SymbolName workerSymbol = new SymbolName(workerName);

        Expression[] expressions = workerReplyStmt.getExpressionList();
        BType[] bTypes = new BType[expressions.length];
        int p = 0;
        for (Expression expression : expressions) {
            expression.accept(this);
            bTypes[p++] = expression.getType();
        }

        workerReplyStmt.setTypes(bTypes);

        if (!workerName.equals("default")) {
            BLangSymbol worker = currentScope.resolve(workerSymbol);
            if (!(worker instanceof Worker)) {
                BLangExceptionHelper.throwSemanticError(expressions[0], SemanticErrors.INCOMPATIBLE_TYPES_UNKNOWN_FOUND,
                        workerSymbol);
            }

            workerReplyStmt.setWorker((Worker) worker);
        }
    }

    @Override
    public void visit(ForkJoinStmt forkJoinStmt) {
        boolean stmtReturns = true;
        //open the fork join statement scope
        openScope(forkJoinStmt);

        // Visit workers
        for (Worker worker : forkJoinStmt.getWorkers()) {
            worker.accept(this);
        }

        // Visit join condition
        ForkJoinStmt.Join join = forkJoinStmt.getJoin();
        openScope(join);
        ParameterDef parameter = join.getJoinResult();
        if (parameter != null) {
            parameter.setKind(VariableDef.Kind.LOCAL_VAR);
            parameter.accept(this);
            join.define(parameter.getSymbolName(), parameter);

            if (!(parameter.getType() instanceof BMapType)) {
                throw new SemanticException("Incompatible types: expected map in " +
                        parameter.getNodeLocation().getFileName() + ":" + parameter.getNodeLocation().
                        getLineNumber());
            }

        }

        // Visit join body
        Statement joinBody = join.getJoinBlock();
        if (joinBody != null) {
            joinBody.accept(this);
            stmtReturns &= joinBody.isAlwaysReturns();
        }
        closeScope();

        // Visit timeout condition
        ForkJoinStmt.Timeout timeout = forkJoinStmt.getTimeout();
        openScope(timeout);
        Expression timeoutExpr = timeout.getTimeoutExpression();
        if (timeoutExpr != null) {
            timeoutExpr.accept(this);
        }

        ParameterDef timeoutParam = timeout.getTimeoutResult();
        if (timeoutParam != null) {
            timeoutParam.accept(this);
            timeout.define(timeoutParam.getSymbolName(), timeoutParam);

            if (!(parameter.getType() instanceof BMapType)) {
                throw new SemanticException("Incompatible types: expected map in " +
                        parameter.getNodeLocation().getFileName() + ":" + parameter.getNodeLocation().getLineNumber());
            }
        }

        // Visit timeout body
        Statement timeoutBody = timeout.getTimeoutBlock();
        if (timeoutBody != null) {
            timeoutBody.accept(this);
            stmtReturns &= timeoutBody.isAlwaysReturns();
        }

        resolveWorkerInteractions(forkJoinStmt);
        resolveForkJoin(forkJoinStmt);
        closeScope();

        forkJoinStmt.setAlwaysReturns(stmtReturns);

        //closing the fork join statement scope
        closeScope();

    }

    @Override
    public void visit(TransactionStmt transactionStmt) {
        transactionStmtCount++;
        transactionStmt.getTransactionBlock().accept(this);
        transactionStmtCount--;
        TransactionStmt.FailedBlock failedBlock = transactionStmt.getFailedBlock();
        if (failedBlock != null) {
            failedBlockCount++;
            failedBlock.getFailedBlockStmt().accept(this);
            failedBlockCount--;
        }
        TransactionStmt.AbortedBlock abortedBlock = transactionStmt.getAbortedBlock();
        if (abortedBlock != null) {
            abortedBlock.getAbortedBlockStmt().accept(this);
        }
        TransactionStmt.CommittedBlock committedBlock = transactionStmt.getCommittedBlock();
        if (committedBlock != null) {
            committedBlock.getCommittedBlockStmt().accept(this);
        }
    }

    @Override
    public void visit(AbortStmt abortStmt) {

    }

    @Override
    public void visit(RetryStmt retryStmt) {
        retryStmt.getRetryCountExpression().accept(this);
        checkRetryStmtValidity(retryStmt);
    }

    @Override
    public void visit(ReplyStmt replyStmt) {
        if (currentCallableUnit instanceof Function) {
            BLangExceptionHelper.throwSemanticError(currentCallableUnit,
                    SemanticErrors.REPLY_STATEMENT_CANNOT_USED_IN_FUNCTION);
        } else if (currentCallableUnit instanceof Action) {
            BLangExceptionHelper.throwSemanticError(currentCallableUnit,
                    SemanticErrors.REPLY_STATEMENT_CANNOT_USED_IN_ACTION);
        }

        if (replyStmt.getReplyExpr() instanceof ActionInvocationExpr) {
            BLangExceptionHelper.throwSemanticError(currentCallableUnit,
                    SemanticErrors.ACTION_INVOCATION_NOT_ALLOWED_IN_REPLY);
        }

        Expression replyExpr = replyStmt.getReplyExpr();
        if (replyExpr != null) {
            visitSingleValueExpr(replyExpr);
            // reply statement supports only message type
            if (replyExpr.getType() != BTypes.typeMessage) {
                BLangExceptionHelper.throwSemanticError(replyExpr, SemanticErrors.INCOMPATIBLE_TYPES,
                        BTypes.typeMessage, replyExpr.getType());
            }
        }
    }

    @Override
    public void visit(ReturnStmt returnStmt) {
        if (currentCallableUnit instanceof Resource) {
            BLangExceptionHelper.throwSemanticError(returnStmt, SemanticErrors.RETURN_CANNOT_USED_IN_RESOURCE);
        }

        if (transactionStmtCount > 0) {
            BLangExceptionHelper.throwSemanticError(returnStmt, SemanticErrors.RETURN_CANNOT_USED_IN_TRANSACTION);
        }

        // Expressions that this return statement contains.
        Expression[] returnArgExprs = returnStmt.getExprs();

        // Return parameters of the current function or actions
        ParameterDef[] returnParamsOfCU = currentCallableUnit.getReturnParameters();

        if (returnArgExprs.length == 0 && returnParamsOfCU.length == 0) {
            // Return stmt has no expressions and function/action does not return anything. Just return.
            return;
        }

        // Return stmt has no expressions, but function/action has returns. Check whether they are named returns
        if (returnArgExprs.length == 0 && returnParamsOfCU[0].getName() != null) {
            // This function/action has named return parameters.
            Expression[] returnExprs = new Expression[returnParamsOfCU.length];
            for (int i = 0; i < returnParamsOfCU.length; i++) {
                SimpleVarRefExpr variableRefExpr = new SimpleVarRefExpr(returnStmt.getNodeLocation(),
                        returnStmt.getWhiteSpaceDescriptor(), returnParamsOfCU[i].getSymbolName().getName(), null,
                        returnParamsOfCU[i].getSymbolName().getPkgPath());
                visit(variableRefExpr);
                returnExprs[i] = variableRefExpr;
            }
            returnStmt.setExprs(returnExprs);
            return;

        } else if (returnArgExprs.length == 0) {
            // This function/action does not contain named return parameters.
            // Therefore this is a semantic error.
            BLangExceptionHelper.throwSemanticError(returnStmt, SemanticErrors.NOT_ENOUGH_ARGUMENTS_TO_RETURN);
        }

        BType[] typesOfReturnExprs = new BType[returnArgExprs.length];
        for (int i = 0; i < returnArgExprs.length; i++) {
            Expression returnArgExpr = returnArgExprs[i];
            returnArgExpr.accept(this);
            typesOfReturnExprs[i] = returnArgExpr.getType();
        }

        // Now check whether this return contains a function invocation expression which returns multiple values
        if (returnArgExprs.length == 1 && returnArgExprs[0] instanceof FunctionInvocationExpr) {
            FunctionInvocationExpr funcIExpr = (FunctionInvocationExpr) returnArgExprs[0];
            // Return types of the function invocations expression
            BType[] funcIExprReturnTypes = funcIExpr.getTypes();
            if (funcIExprReturnTypes.length > returnParamsOfCU.length) {
                BLangExceptionHelper.throwSemanticError(returnStmt, SemanticErrors.TOO_MANY_ARGUMENTS_TO_RETURN);

            } else if (funcIExprReturnTypes.length < returnParamsOfCU.length) {
                BLangExceptionHelper.throwSemanticError(returnStmt, SemanticErrors.NOT_ENOUGH_ARGUMENTS_TO_RETURN);

            }

            for (int i = 0; i < returnParamsOfCU.length; i++) {
                BType lhsType = returnParamsOfCU[i].getType();
                BType rhsType = funcIExprReturnTypes[i];

                // Check whether the right-hand type can be assigned to the left-hand type.
                if (isAssignableTo(lhsType, rhsType)) {
                    continue;
                }

                // TODO Check whether an implicit cast is possible
                // This requires a tree rewrite. Off the top of my head the results of function or action invocation
                // should be stored in temporary variables with matching types. Then these temporary variables can be
                // assigned to left-hand side expressions one by one.

                BLangExceptionHelper.throwSemanticError(returnStmt,
                        SemanticErrors.CANNOT_USE_TYPE_IN_RETURN_STATEMENT, lhsType, rhsType);
            }

            return;
        }

        if (typesOfReturnExprs.length > returnParamsOfCU.length) {
            BLangExceptionHelper.throwSemanticError(returnStmt, SemanticErrors.TOO_MANY_ARGUMENTS_TO_RETURN);

        } else if (typesOfReturnExprs.length < returnParamsOfCU.length) {
            BLangExceptionHelper.throwSemanticError(returnStmt, SemanticErrors.NOT_ENOUGH_ARGUMENTS_TO_RETURN);

        } else {
            // Now we know that lengths for both arrays are equal.
            // Let's check their types
            for (int i = 0; i < returnParamsOfCU.length; i++) {
                // Except for the first argument in return statement, check for FunctionInvocationExprs which return
                // multiple values.
                if (returnArgExprs[i] instanceof FunctionInvocationExpr) {
                    FunctionInvocationExpr funcIExpr = ((FunctionInvocationExpr) returnArgExprs[i]);
                    if (funcIExpr.getTypes().length > 1) {
                        BLangExceptionHelper.throwSemanticError(returnStmt,
                                SemanticErrors.MULTIPLE_VALUE_IN_SINGLE_VALUE_CONTEXT,
                                funcIExpr.getCallableUnit().getName());
                    }
                }

                BType lhsType = returnParamsOfCU[i].getType();
                BType rhsType = typesOfReturnExprs[i];

                // Check type assignability
                AssignabilityResult result = performAssignabilityCheck(lhsType, returnArgExprs[i]);
                if (result.expression != null) {
                    returnArgExprs[i] = result.expression;
                } else if (!result.assignable) {
                    BLangExceptionHelper.throwSemanticError(returnStmt,
                            SemanticErrors.CANNOT_USE_TYPE_IN_RETURN_STATEMENT, lhsType, rhsType);
                }
            }
        }
    }

    @Override
    public void visit(TransformStmt transformStmt) {
        BlockStmt blockStmt = transformStmt.getBody();
        if (blockStmt.getStatements().length == 0) {
            BLangExceptionHelper.throwSemanticError(transformStmt, SemanticErrors.TRANSFORM_STATEMENT_NO_BODY);
        }
        blockStmt.accept(this);
    }

    // Expressions

    @Override
    public void visit(InstanceCreationExpr instanceCreationExpr) {
        visitSingleValueExpr(instanceCreationExpr);

        if (BTypes.isValueType(instanceCreationExpr.getType())) {
            BLangExceptionHelper.throwSemanticError(instanceCreationExpr,
                    SemanticErrors.CANNOT_USE_CREATE_FOR_VALUE_TYPES, instanceCreationExpr.getType());
        }
        // TODO here the type shouldn't be a value type
//        Expression expr = instanceCreationExpr.getRExpr();
//        expr.accept(this);

    }

    @Override
    public void visit(FunctionInvocationExpr funcIExpr) {
        Expression[] exprs = funcIExpr.getArgExprs();
        for (Expression expr : exprs) {
            visitSingleValueExpr(expr);
        }

        linkFunction(funcIExpr);

        //Find the return types of this function invocation expression.
        if (funcIExpr.isFunctionPointerInvocation()) {
            BFunctionType type = (BFunctionType) funcIExpr.getFunctionPointerVariableDef().getType();
            funcIExpr.setTypes(type.getReturnParameterType());
        } else {
            BType[] returnParamTypes = funcIExpr.getCallableUnit().getReturnParamTypes();
            funcIExpr.setTypes(returnParamTypes);
        }
    }

    // TODO Duplicate code. fix me
    @Override
    public void visit(ActionInvocationExpr actionIExpr) {

        String pkgPath = actionIExpr.getPackagePath();
        String name = actionIExpr.getConnectorName();

        // First check action invocation happens on a variable def
        SymbolName symbolName = new SymbolName(name, pkgPath);
        BLangSymbol bLangSymbol = currentScope.resolve(symbolName);

        if (bLangSymbol instanceof SimpleVariableDef) {
            if (((SimpleVariableDef) bLangSymbol).getType() instanceof StructDef) {
                // This is not a action invocation, but possible function pointer invocation inside a struct.
                // TODO : Fix this logic and remove action invocation.
                StructDef structDef = (StructDef) ((SimpleVariableDef) bLangSymbol).getType();
                VariableDef matchingVariableDef = null;
                for (VariableDefStmt variableDefStmt : structDef.getFieldDefStmts()) {
                    VariableDef variableDef = variableDefStmt.getVariableDef();
                    if (variableDef.getType() instanceof BFunctionType &&
                            variableDef.getIdentifier().getName().equals(actionIExpr.getName())) {
                        matchingVariableDef = variableDef;
                        break;
                    }
                }
                if (matchingVariableDef == null) {
                    throw BLangExceptionHelper.getSemanticError(actionIExpr.getNodeLocation(),
                            SemanticErrors.UNDEFINED_FUNCTION, actionIExpr.getName());
                }
                BFunctionType functionType = (BFunctionType) matchingVariableDef.getType();
                Expression[] exprs = actionIExpr.getArgExprs();
                if (exprs == null || functionType.getParameterType().length != exprs.length) {
                    throw BLangExceptionHelper.getSemanticError(actionIExpr.getNodeLocation(),
                            SemanticErrors.INCORRECT_FUNCTION_ARGUMENTS, actionIExpr.getName());
                }
                for (Expression expr : exprs) {
                    visitSingleValueExpr(expr);
                }
                for (int i = 0; i < exprs.length; i++) {
                    if (!isAssignableTo(exprs[i].getType(), functionType.getParameterType()[i])) {
                        throw BLangExceptionHelper.getSemanticError(actionIExpr.getNodeLocation(),
                                SemanticErrors.INCORRECT_FUNCTION_ARGUMENTS, actionIExpr.getName());
                    }
                }
                actionIExpr.setTypes(functionType.getReturnParameterType());
                actionIExpr.setFunctionInvocation(true);
                actionIExpr.setVariableDef((SimpleVariableDef) bLangSymbol);
                actionIExpr.setFieldDef(matchingVariableDef);
                return;
            }
            // Process as Action invocation.
            if (!(((SimpleVariableDef) bLangSymbol).getType() instanceof BallerinaConnectorDef)) {
                throw BLangExceptionHelper.getSemanticError(actionIExpr.getNodeLocation(),
                        SemanticErrors.INCORRECT_ACTION_INVOCATION);
            }

            Expression[] exprs = new Expression[actionIExpr.getArgExprs().length + 1];
            SimpleVarRefExpr variableRefExpr = new SimpleVarRefExpr(actionIExpr.getNodeLocation(),
                    null, name, null, pkgPath);
            exprs[0] = variableRefExpr;
            for (int i = 0; i < actionIExpr.getArgExprs().length; i++) {
                exprs[i + 1] = actionIExpr.getArgExprs()[i];
            }
            actionIExpr.setArgExprs(exprs);
            SimpleVariableDef varDef = (SimpleVariableDef) bLangSymbol;
            actionIExpr.setConnectorName(varDef.getTypeName().getName());
            actionIExpr.setPackageName(varDef.getTypeName().getPackageName());
            actionIExpr.setPackagePath(varDef.getTypeName().getPackagePath());
        } else if (bLangSymbol instanceof BallerinaConnectorDef) {
            throw BLangExceptionHelper.getSemanticError(actionIExpr.getNodeLocation(),
                    SemanticErrors.INVALID_ACTION_INVOCATION);
        }

        Expression[] exprs = actionIExpr.getArgExprs();
        for (Expression expr : exprs) {
            visitSingleValueExpr(expr);
        }

        linkAction(actionIExpr);

        //Find the return types of this function invocation expression.
        BType[] returnParamTypes = actionIExpr.getCallableUnit().getReturnParamTypes();
        actionIExpr.setTypes(returnParamTypes);
    }

    @Override
    public void visit(BasicLiteral basicLiteral) {
        BType bType = BTypes.resolveType(basicLiteral.getTypeName(), currentScope, basicLiteral.getNodeLocation());
        basicLiteral.setType(bType);
    }

    @Override
    public void visit(DivideExpr divideExpr) {
        BType binaryExprType = verifyBinaryArithmeticExprType(divideExpr);
        validateBinaryExprTypeForIntFloat(divideExpr, binaryExprType);
    }

    @Override
    public void visit(ModExpression modExpr) {
        BType binaryExprType = verifyBinaryArithmeticExprType(modExpr);
        validateBinaryExprTypeForIntFloat(modExpr, binaryExprType);
    }

    @Override
    public void visit(UnaryExpression unaryExpr) {
        visitSingleValueExpr(unaryExpr.getRExpr());
        unaryExpr.setType(unaryExpr.getRExpr().getType());

        if (Operator.SUB.equals(unaryExpr.getOperator()) || Operator.ADD.equals(unaryExpr.getOperator())) {
            if (unaryExpr.getType() != BTypes.typeInt && unaryExpr.getType() != BTypes.typeFloat) {
                throwInvalidUnaryOpError(unaryExpr);
            }

        } else if (Operator.NOT.equals(unaryExpr.getOperator())) {
            if (unaryExpr.getType() != BTypes.typeBoolean) {
                throwInvalidUnaryOpError(unaryExpr);
            }

        } else if (Operator.TYPEOF.equals(unaryExpr.getOperator())) {
            unaryExpr.setType(BTypes.typeType);
        } else if (Operator.LENGTHOF.equals(unaryExpr.getOperator())) {
            BType rType = unaryExpr.getRExpr().getType();
            if (!((rType instanceof BArrayType) || (rType == BTypes.typeJSON))) {
                throwInvalidUnaryOpError(unaryExpr);
            }
            unaryExpr.setType(BTypes.typeInt);
        } else {
            BLangExceptionHelper.throwSemanticError(unaryExpr, SemanticErrors.UNKNOWN_OPERATOR_IN_UNARY,
                    unaryExpr.getOperator());
        }
    }

    @Override
    public void visit(AddExpression addExpr) {
        BType binaryExprType = verifyBinaryArithmeticExprType(addExpr);
        if (binaryExprType != BTypes.typeInt &&
                binaryExprType != BTypes.typeFloat &&
                binaryExprType != BTypes.typeString &&
                binaryExprType != BTypes.typeXML) {
            throwInvalidBinaryOpError(addExpr);
        }
    }

    @Override
    public void visit(MultExpression multExpr) {
        BType binaryExprType = verifyBinaryArithmeticExprType(multExpr);
        validateBinaryExprTypeForIntFloat(multExpr, binaryExprType);
    }

    @Override
    public void visit(SubtractExpression subtractExpr) {
        BType binaryExprType = verifyBinaryArithmeticExprType(subtractExpr);
        validateBinaryExprTypeForIntFloat(subtractExpr, binaryExprType);
    }

    @Override
    public void visit(AndExpression andExpr) {
        visitBinaryLogicalExpr(andExpr);
    }

    @Override
    public void visit(OrExpression orExpr) {
        visitBinaryLogicalExpr(orExpr);
    }

    @Override
    public void visit(EqualExpression equalExpr) {
        verifyBinaryEqualityExprType(equalExpr);
    }

    @Override
    public void visit(NotEqualExpression notEqualExpr) {
        verifyBinaryEqualityExprType(notEqualExpr);
    }

    @Override
    public void visit(GreaterEqualExpression greaterEqualExpr) {
        BType compareExprType = verifyBinaryCompareExprType(greaterEqualExpr);
        validateBinaryExprTypeForIntFloat(greaterEqualExpr, compareExprType);
    }

    @Override
    public void visit(GreaterThanExpression greaterThanExpr) {
        BType compareExprType = verifyBinaryCompareExprType(greaterThanExpr);
        validateBinaryExprTypeForIntFloat(greaterThanExpr, compareExprType);
    }

    @Override
    public void visit(LessEqualExpression lessEqualExpr) {
        BType compareExprType = verifyBinaryCompareExprType(lessEqualExpr);
        validateBinaryExprTypeForIntFloat(lessEqualExpr, compareExprType);
    }

    @Override
    public void visit(LessThanExpression lessThanExpr) {
        BType compareExprType = verifyBinaryCompareExprType(lessThanExpr);
        validateBinaryExprTypeForIntFloat(lessThanExpr, compareExprType);
    }

    @Override
    public void visit(RefTypeInitExpr refTypeInitExpr) {
        visitMapJsonInitExpr(refTypeInitExpr);
    }

    @Override
    public void visit(MapInitExpr mapInitExpr) {
        visitMapJsonInitExpr(mapInitExpr);
    }

    @Override
    public void visit(JSONInitExpr jsonInitExpr) {
        visitMapJsonInitExpr(jsonInitExpr);
    }

    @Override
    public void visit(JSONArrayInitExpr jsonArrayInitExpr) {
        BType inheritedType = jsonArrayInitExpr.getInheritedType();
        jsonArrayInitExpr.setType(inheritedType);

        BType inheritedElementType;
        if (inheritedType instanceof BArrayType) {
            inheritedElementType = ((BArrayType) inheritedType).getElementType();
        } else {
            inheritedElementType = inheritedType;
        }

        Expression[] argExprs = jsonArrayInitExpr.getArgExprs();

        for (int i = 0; i < argExprs.length; i++) {
            Expression argExpr = argExprs[i];
            if (argExpr instanceof RefTypeInitExpr) {
                argExpr = getNestedInitExpr(argExpr, inheritedElementType);
                argExprs[i] = argExpr;
            }
            visitSingleValueExpr(argExpr);

            // check the type compatibility of the value.
            BType argExprType = argExpr.getType();
            if (BTypes.isValueType(argExprType)) {
                TypeCastExpression typeCastExpr = checkWideningPossible(BTypes.typeJSON, argExpr);
                if (typeCastExpr != null) {
                    argExprs[i] = typeCastExpr;
                } else {
                    BLangExceptionHelper.throwSemanticError(argExpr,
                            SemanticErrors.INCOMPATIBLE_TYPES_CANNOT_CONVERT, argExprType.getSymbolName(),
                            inheritedType.getSymbolName());
                }
                continue;
            }

            if (argExprType != BTypes.typeNull && isAssignableTo(inheritedElementType, argExprType)) {
                continue;
            }

            TypeCastExpression typeCastExpr = checkWideningPossible(inheritedElementType, argExpr);
            if (typeCastExpr == null) {
                BLangExceptionHelper.throwSemanticError(jsonArrayInitExpr,
                        SemanticErrors.INCOMPATIBLE_TYPES_CANNOT_CONVERT, argExpr.getType(), inheritedElementType);
            }
            argExprs[i] = typeCastExpr;
        }
    }

    @Override
    public void visit(ConnectorInitExpr connectorInitExpr) {
        BType inheritedType = connectorInitExpr.getInheritedType();
        boolean isFilterConnector = ((BallerinaConnectorDef) inheritedType).isFilterConnector();
        if (!(inheritedType instanceof BallerinaConnectorDef)) {
            BLangExceptionHelper.throwSemanticError(connectorInitExpr, SemanticErrors.CONNECTOR_INIT_NOT_ALLOWED);
        }
        connectorInitExpr.setType(inheritedType);
        Expression[] argExprs = connectorInitExpr.getArgExprs();
        ParameterDef[] parameterDefs = ((BallerinaConnectorDef) inheritedType).getParameterDefs();

        // if this is a normal connector, arguments count should match to the parameter defs count.
        // if this is a filter connector, arguments count should be one less than the parameter defs count.
        if ((!isFilterConnector && argExprs.length != parameterDefs.length)
                || (isFilterConnector && argExprs.length != parameterDefs.length - 1)) {
            BLangExceptionHelper.throwSemanticError(connectorInitExpr, SemanticErrors.ARGUMENTS_COUNT_MISMATCH,
                    parameterDefs.length, argExprs.length);
        }

        for (Expression argExpr : argExprs) {
            visitSingleValueExpr(argExpr);
        }

        for (int i = 0; i < argExprs.length; i++) {
            int j = i;
            if (isFilterConnector) {
                j += 1;
            }
            SimpleTypeName simpleTypeName = parameterDefs[j].getTypeName();
            BType paramType = BTypes.resolveType(simpleTypeName, currentScope, connectorInitExpr.getNodeLocation());
            parameterDefs[j].setType(paramType);

            Expression argExpr = argExprs[i];
            AssignabilityResult result = performAssignabilityCheck(parameterDefs[j].getType(), argExpr);
            if (result.expression != null) {
                argExprs[i] = result.expression;
            } else if (!result.assignable) {
                BLangExceptionHelper.throwSemanticError(connectorInitExpr, SemanticErrors.INCOMPATIBLE_TYPES,
                        parameterDefs[j].getType(), argExpr.getType());
            }
        }

        ConnectorInitExpr filterConnectorInitExpr = connectorInitExpr.getParentConnectorInitExpr();
        if (filterConnectorInitExpr != null) {
            visit(filterConnectorInitExpr);
            BType filterConnectorType = filterConnectorInitExpr.getFilterSupportedType();
            // Resolve reference connector type if this is a filter connector
            if (filterConnectorType != null && filterConnectorType instanceof BallerinaConnectorDef) {
                if (!filterConnectorType.equals(inheritedType)) {
                    BLangExceptionHelper.throwSemanticError(connectorInitExpr,
                            SemanticErrors.CONNECTOR_TYPES_NOT_EQUIVALENT,
                            inheritedType, filterConnectorInitExpr.getInheritedType());
                }
            }
        }
    }

    @Override
    public void visit(ArrayInitExpr arrayInitExpr) {
        if (!(arrayInitExpr.getInheritedType() instanceof BArrayType)) {
            BLangExceptionHelper.throwSemanticError(arrayInitExpr, SemanticErrors.ARRAY_INIT_NOT_ALLOWED_HERE);
        }

        visitArrayInitExpr(arrayInitExpr);
    }

    private void visitArrayInitExpr(ArrayInitExpr arrayInitExpr) {
        BType inheritedType = arrayInitExpr.getInheritedType();
        arrayInitExpr.setType(inheritedType);
        Expression[] argExprs = arrayInitExpr.getArgExprs();
        if (argExprs.length == 0) {
            return;
        }

        BType expectedElementType = ((BArrayType) inheritedType).getElementType();
        for (int i = 0; i < argExprs.length; i++) {
            Expression argExpr = argExprs[i];
            if (argExpr instanceof RefTypeInitExpr) {
                ((RefTypeInitExpr) argExpr).setInheritedType(expectedElementType);
                argExpr = getNestedInitExpr(argExpr, expectedElementType);
                argExprs[i] = argExpr;
            }

            visitSingleValueExpr(argExpr);
            AssignabilityResult result = performAssignabilityCheck(expectedElementType, argExpr);
            if (result.expression != null) {
                argExprs[i] = result.expression;
            } else if (!result.assignable) {
                BLangExceptionHelper.throwSemanticError(argExpr, SemanticErrors.INCOMPATIBLE_ASSIGNMENT,
                        argExpr.getType(), expectedElementType);
            }
        }
    }

    /**
     * Visit and analyze ballerina Struct initializing expression.
     */
    @Override
    public void visit(StructInitExpr structInitExpr) {
        BType inheritedType = structInitExpr.getInheritedType();
        structInitExpr.setType(inheritedType);
        Expression[] argExprs = structInitExpr.getArgExprs();
        if (argExprs.length == 0) {
            return;
        }

        StructDef structDef = (StructDef) inheritedType;
        for (Expression argExpr : argExprs) {
            KeyValueExpr keyValueExpr = (KeyValueExpr) argExpr;
            Expression keyExpr = keyValueExpr.getKeyExpr();
            if (!(keyExpr instanceof SimpleVarRefExpr)) {
                throw BLangExceptionHelper.getSemanticError(keyExpr.getNodeLocation(),
                        SemanticErrors.INVALID_FIELD_NAME_STRUCT_INIT);
            }

            SimpleVarRefExpr varRefExpr = (SimpleVarRefExpr) keyExpr;
            //TODO fix properly package conflict
            BLangSymbol varDefSymbol = structDef.resolveMembers(new SymbolName(varRefExpr.getSymbolName().getName(),
                    structDef.getPackagePath()));

            if (varDefSymbol == null) {
                throw BLangExceptionHelper.getSemanticError(keyExpr.getNodeLocation(),
                        SemanticErrors.UNKNOWN_FIELD_IN_STRUCT, varRefExpr.getVarName(), structDef.getName());
            }

            if (!(varDefSymbol instanceof SimpleVariableDef)) {
                throw BLangExceptionHelper.getSemanticError(varRefExpr.getNodeLocation(),
                        SemanticErrors.INCOMPATIBLE_TYPES_UNKNOWN_FOUND, varDefSymbol.getSymbolName());
            }

            SimpleVariableDef varDef = (SimpleVariableDef) varDefSymbol;
            varRefExpr.setVariableDef(varDef);

            BType structFieldType = varDef.getType();
            Expression valueExpr = keyValueExpr.getValueExpr();
            if (valueExpr instanceof RefTypeInitExpr) {
                valueExpr = getNestedInitExpr(valueExpr, structFieldType);
                keyValueExpr.setValueExpr(valueExpr);
            }

            valueExpr.accept(this);

            // Check whether the right-hand type can be assigned to the left-hand type.
            AssignabilityResult result = performAssignabilityCheck(structFieldType, valueExpr);
            if (result.expression != null) {
                valueExpr = result.expression;
                keyValueExpr.setValueExpr(valueExpr);
            } else if (!result.assignable) {
                BLangExceptionHelper.throwSemanticError(keyExpr, SemanticErrors.INCOMPATIBLE_TYPES,
                        varDef.getType(), valueExpr.getType());
            }
        }
    }

    @Override
    public void visit(KeyValueExpr keyValueExpr) {

    }

    @Override
    public void visit(SimpleVarRefExpr simpleVarRefExpr) {
        // Resolve package path from the give package name
        if (simpleVarRefExpr.getPkgName() != null && simpleVarRefExpr.getPkgPath() == null) {
            throw BLangExceptionHelper.getSemanticError(simpleVarRefExpr.getNodeLocation(),
                    SemanticErrors.UNDEFINED_PACKAGE_NAME, simpleVarRefExpr.getPkgName(),
                    simpleVarRefExpr.getPkgName() + ":" + simpleVarRefExpr.getVarName());
        }

        SymbolName symbolName = simpleVarRefExpr.getSymbolName();
        // Check whether this symName is declared
        BLangSymbol varDefSymbol = currentScope.resolve(symbolName);
        if (varDefSymbol == null) {
            BLangExceptionHelper.throwSemanticError(simpleVarRefExpr, SemanticErrors.UNDEFINED_SYMBOL,
                    symbolName);
        }

        if (!(varDefSymbol instanceof VariableDef)) {
            throw BLangExceptionHelper.getSemanticError(simpleVarRefExpr.getNodeLocation(),
                    SemanticErrors.INCOMPATIBLE_TYPES_UNKNOWN_FOUND, symbolName);
        }

        simpleVarRefExpr.setVariableDef((VariableDef) varDefSymbol);
    }

    @Override
    public void visit(FieldBasedVarRefExpr fieldBasedVarRefExpr) {
        String fieldName = fieldBasedVarRefExpr.getFieldName();
        VariableReferenceExpr varRefExpr = fieldBasedVarRefExpr.getVarRefExpr();
        varRefExpr.accept(this);

        // Type of the varRefExpr can be either Struct, Map, JSON, Array
        BType varRefType = varRefExpr.getType();
        if (varRefType instanceof StructDef) {
            StructDef structDef = (StructDef) varRefType;
            BLangSymbol fieldSymbol = structDef.resolveMembers(new SymbolName(fieldName, structDef.getPackagePath()));
            if (fieldSymbol == null) {
                throw BLangExceptionHelper.getSemanticError(varRefExpr.getNodeLocation(),
                        SemanticErrors.UNKNOWN_FIELD_IN_STRUCT, fieldName, structDef.getName());
            }
            SimpleVariableDef fieldDef = (SimpleVariableDef) fieldSymbol;
            fieldBasedVarRefExpr.setFieldDef(fieldDef);
            fieldBasedVarRefExpr.setType(fieldDef.getType());

        } else if (varRefType == BTypes.typeMap) {
            fieldBasedVarRefExpr.setType(((BMapType) varRefType).getElementType());

        } else if (varRefType == BTypes.typeJSON) {
            fieldBasedVarRefExpr.setType(BTypes.typeJSON);
        } else if (varRefType instanceof BJSONConstraintType) {
            StructDef structDefReference = (StructDef) ((BJSONConstraintType) varRefType).getConstraint();
            BLangSymbol fieldSymbol = structDefReference.resolveMembers(
                    new SymbolName(fieldName, structDefReference.getPackagePath()));
            if (fieldSymbol == null) {
                throw BLangExceptionHelper
                        .getSemanticError(varRefExpr.getNodeLocation(), SemanticErrors.UNKNOWN_FIELD_IN_JSON_STRUCT,
                                fieldName, structDefReference.getName());
            }
            VariableDef fieldDef = (VariableDef) fieldSymbol;
            fieldBasedVarRefExpr.setFieldDef(fieldDef);
            fieldBasedVarRefExpr.setType(BTypes.typeJSON);
        } else if (varRefType instanceof BArrayType && fieldName.equals("length")) {
            if (fieldBasedVarRefExpr.isLHSExpr()) {
                //cannot assign a value to array length
                throw BLangExceptionHelper.getSemanticError(fieldBasedVarRefExpr.getNodeLocation(),
                        SemanticErrors.CANNOT_ASSIGN_VALUE_ARRAY_LENGTH);

            }
            fieldBasedVarRefExpr.setType(BTypes.typeInt);

        } else {
            throw BLangExceptionHelper.getSemanticError(varRefExpr.getNodeLocation(),
                    SemanticErrors.INVALID_OPERATION_NOT_SUPPORT_INDEXING, varRefType);
            // TODO Implement .type expression
        }
    }

    @Override
    public void visit(IndexBasedVarRefExpr indexBasedVarRefExpr) {
        Expression indexExpr = indexBasedVarRefExpr.getIndexExpr();
        indexExpr.accept(this);

        VariableReferenceExpr varRefExpr = indexBasedVarRefExpr.getVarRefExpr();
        varRefExpr.accept(this);

        // Type of the varRefExpr can be either Array, Map, JSON, Struct.
        BType varRefType = varRefExpr.getType();
        if (varRefType instanceof BArrayType) {
            if (indexExpr.getType() != BTypes.typeInt) {
                throw BLangExceptionHelper.getSemanticError(indexExpr.getNodeLocation(),
                        SemanticErrors.NON_INTEGER_ARRAY_INDEX, indexExpr.getType());
            }
            BArrayType arrayType = (BArrayType) varRefType;
            indexBasedVarRefExpr.setType(arrayType.getElementType());

        } else if (varRefType == BTypes.typeMap) {
            if (indexExpr.getType() != BTypes.typeString) {
                throw BLangExceptionHelper.getSemanticError(indexExpr.getNodeLocation(),
                        SemanticErrors.NON_STRING_MAP_INDEX, indexExpr.getType());
            }
            BMapType mapType = (BMapType) varRefType;
            indexBasedVarRefExpr.setType(mapType.getElementType());

        } else if (varRefType.getTag() == TypeTags.C_JSON_TAG) {
            throw BLangExceptionHelper.getSemanticError(indexExpr.getNodeLocation(),
                    SemanticErrors.INVALID_OPERATION_NOT_SUPPORT_INDEXING, varRefExpr.getType().toString());
        } else if (varRefType == BTypes.typeJSON) {
            if (indexExpr.getType() != BTypes.typeInt && indexExpr.getType() != BTypes.typeString) {
                throw BLangExceptionHelper.getSemanticError(indexExpr.getNodeLocation(),
                        SemanticErrors.INCOMPATIBLE_TYPES, "string or int", varRefExpr.getType());
            }
            indexBasedVarRefExpr.setType(BTypes.typeJSON);

        } else if (varRefType instanceof StructDef) {
            if (indexExpr.getType() != BTypes.typeString) {
                throw BLangExceptionHelper.getSemanticError(indexExpr.getNodeLocation(),
                        SemanticErrors.INCOMPATIBLE_TYPES, BTypes.typeString, varRefExpr.getType());
            }

            if (!(indexExpr instanceof BasicLiteral)) {
                throw BLangExceptionHelper.getSemanticError(indexExpr.getNodeLocation(),
                        SemanticErrors.DYNAMIC_KEYS_NOT_SUPPORTED_FOR_STRUCT);
            }

            String fieldName = ((BasicLiteral) indexExpr).getBValue().stringValue();
            StructDef structDef = (StructDef) varRefType;
            BLangSymbol fieldSymbol = structDef.resolveMembers(new SymbolName(fieldName, structDef.getPackagePath()));
            if (fieldSymbol == null) {
                throw BLangExceptionHelper.getSemanticError(varRefExpr.getNodeLocation(),
                        SemanticErrors.UNKNOWN_FIELD_IN_STRUCT, fieldName, structDef.getName());
            }
            SimpleVariableDef fieldDef = (SimpleVariableDef) fieldSymbol;
            indexBasedVarRefExpr.setFieldDef(fieldDef);
            indexBasedVarRefExpr.setType(fieldDef.getType());

        } else {
            throw BLangExceptionHelper.getSemanticError(indexBasedVarRefExpr.getNodeLocation(),
                    SemanticErrors.INVALID_OPERATION_NOT_SUPPORT_INDEXING, varRefType);
        }
    }

    @Override
    public void visit(XMLAttributesRefExpr xmlAttributesRefExpr) {
        VariableReferenceExpr varRefExpr = xmlAttributesRefExpr.getVarRefExpr();
        varRefExpr.accept(this);

        if (varRefExpr.getType() != BTypes.typeXML) {
            BLangExceptionHelper.throwSemanticError(xmlAttributesRefExpr, SemanticErrors.INCOMPATIBLE_TYPES,
                    BTypes.typeXML, varRefExpr.getType());
        }

        Expression indexExpr = xmlAttributesRefExpr.getIndexExpr();
        if (indexExpr == null) {
            if (xmlAttributesRefExpr.isLHSExpr()) {
                BLangExceptionHelper.throwSemanticError(xmlAttributesRefExpr,
                        SemanticErrors.XML_ATTRIBUTE_MAP_UPDATE_NOT_ALLOWED);
            }
            xmlAttributesRefExpr.setType(BTypes.typeXMLAttributes);
            return;
        }

        xmlAttributesRefExpr.setType(BTypes.typeString);
        indexExpr.accept(this);
        if (indexExpr instanceof XMLQNameExpr) {
            ((XMLQNameExpr) indexExpr).setUsedInXML(true);
            return;
        }

        if (indexExpr.getType() != BTypes.typeString) {
            BLangExceptionHelper.throwSemanticError(indexExpr, SemanticErrors.NON_STRING_MAP_INDEX,
                    indexExpr.getType());
        }

        Map<String, Expression> namespaces = getNamespaceInScope(xmlAttributesRefExpr.getNodeLocation());
        xmlAttributesRefExpr.setNamespaces(namespaces);
    }

    @Override
    public void visit(XMLQNameExpr xmlQNameRefExpr) {
        if (xmlQNameRefExpr.isLHSExpr()) {
            BLangExceptionHelper.throwSemanticError(xmlQNameRefExpr, SemanticErrors.XML_QNAME_UPDATE_NOT_ALLOWED);
        }

        xmlQNameRefExpr.setType(BTypes.typeString);
        String prefix = xmlQNameRefExpr.getPrefix();
        if (prefix.isEmpty()) {
            return;
        }

        if (prefix.equals(XMLConstants.XMLNS_ATTRIBUTE)) {
            BLangExceptionHelper.throwSemanticError(xmlQNameRefExpr, SemanticErrors.INVALID_NAMESPACE_PREFIX, prefix);
        }

        NamespaceSymbolName nsSymbolName = new NamespaceSymbolName(prefix);
        BLangSymbol symbol = currentScope.resolve(nsSymbolName);

        if (symbol == null) {
            BLangExceptionHelper.throwSemanticError(xmlQNameRefExpr, SemanticErrors.UNDEFINED_NAMESPACE, prefix);
        }

        String namepsaceUri = ((NamespaceDeclaration) symbol).getNamespaceUri();
        BasicLiteral namespaceUriLiteral = new BasicLiteral(xmlQNameRefExpr.getNodeLocation(), null,
                new SimpleTypeName(TypeConstants.STRING_TNAME), new BString(namepsaceUri));
        namespaceUriLiteral.accept(this);
        xmlQNameRefExpr.setNamepsaceUri(namespaceUriLiteral);
    }

    @Override
    public void visit(TypeCastExpression typeCastExpr) {
        // Evaluate the expression and set the type
        boolean isMultiReturn = typeCastExpr.isMultiReturnExpr();
        Expression rExpr = typeCastExpr.getRExpr();
        visitSingleValueExpr(rExpr);

        BType sourceType = rExpr.getType();
        BType targetType = typeCastExpr.getType();
        if (targetType == null) {
            targetType = BTypes.resolveType(typeCastExpr.getTypeName(), currentScope, typeCastExpr.getNodeLocation());
            typeCastExpr.setType(targetType);
        }

        // casting to function pointer is not supported in this 0.9 release. issue #2944
        if (sourceType instanceof BFunctionType || targetType instanceof BFunctionType) {
            BLangExceptionHelper.throwSemanticError(typeCastExpr, SemanticErrors.INCOMPATIBLE_TYPES_CANNOT_CAST,
                    sourceType, targetType);
        }

        // casting a null literal is not supported.
        if (rExpr instanceof NullLiteral) {
            BLangExceptionHelper.throwSemanticError(typeCastExpr, SemanticErrors.INCOMPATIBLE_TYPES_CANNOT_CAST,
                    sourceType, targetType);
        }

        // Find the eval function from explicit casting lattice
        TypeEdge newEdge = TypeLattice.getExplicitCastLattice().getEdgeFromTypes(sourceType, targetType, null);
        if (newEdge != null) {
            typeCastExpr.setOpcode(newEdge.getOpcode());

            if (!newEdge.isSafe() && !isMultiReturn) {
                BLangExceptionHelper.throwSemanticError(typeCastExpr, SemanticErrors.UNSAFE_CAST_ATTEMPT,
                        sourceType, targetType);
            }

            if (!isMultiReturn) {
                typeCastExpr.setTypes(new BType[]{targetType});
                return;
            }

        } else if (sourceType == targetType) {
            typeCastExpr.setOpcode(InstructionCodes.NOP);
            if (!isMultiReturn) {
                typeCastExpr.setTypes(new BType[]{targetType});
                return;
            }

        } else if ((sourceType.getTag() == TypeTags.C_JSON_TAG && targetType.getTag() == TypeTags.C_JSON_TAG)
                && TypeLattice.isAssignCompatible((StructDef) ((BJSONConstraintType) targetType).getConstraint(),
                (StructDef) ((BJSONConstraintType) sourceType).getConstraint())) {
            typeCastExpr.setOpcode(InstructionCodes.NOP);
            if (!isMultiReturn) {
                typeCastExpr.setTypes(new BType[]{targetType});
                return;
            }
        } else {
            boolean isUnsafeCastPossible = false;
            if (isMultiReturn) {
                isUnsafeCastPossible = checkUnsafeCastPossible(sourceType, targetType);
            }

            if (isUnsafeCastPossible) {
                typeCastExpr.setOpcode(InstructionCodes.CHECKCAST);
            } else {
                TypeEdge conversionEdge = TypeLattice.getTransformLattice().getEdgeFromTypes(sourceType,
                        targetType, null);
                if (conversionEdge != null) {
                    throw BLangExceptionHelper.getSemanticError(typeCastExpr.getNodeLocation(),
                            SemanticErrors.CANNOT_CAST_WITH_SUGGESTION, sourceType, targetType);
                }
                throw BLangExceptionHelper.getSemanticError(typeCastExpr.getNodeLocation(),
                        SemanticErrors.INCOMPATIBLE_TYPES_CANNOT_CAST, sourceType, targetType);
            }
        }

        // If this is a multi-value return conversion expression, set the return types.
        BLangSymbol error = currentScope.resolve(new SymbolName(BALLERINA_CAST_ERROR, ERRORS_PACKAGE));
        if (error == null || !(error instanceof StructDef)) {
            BLangExceptionHelper.throwSemanticError(typeCastExpr,
                    SemanticErrors.CANNOT_RESOLVE_STRUCT, ERRORS_PACKAGE, BALLERINA_CAST_ERROR);
        }
        typeCastExpr.setTypes(new BType[]{targetType, (BType) error});
    }


    @Override
    public void visit(TypeConversionExpr typeConversionExpr) {
        // Evaluate the expression and set the type
        boolean isMultiReturn = typeConversionExpr.isMultiReturnExpr();
        Expression rExpr = typeConversionExpr.getRExpr();
        visitSingleValueExpr(rExpr);

        BType sourceType = rExpr.getType();
        BType targetType = typeConversionExpr.getType();
        if (targetType == null) {
            targetType = BTypes.resolveType(typeConversionExpr.getTypeName(), currentScope, null);
            typeConversionExpr.setType(targetType);
        }

        // casting a null literal is not supported.
        if (rExpr instanceof NullLiteral) {
            BLangExceptionHelper.throwSemanticError(typeConversionExpr,
                    SemanticErrors.INCOMPATIBLE_TYPES_CANNOT_CONVERT, sourceType, targetType);
        }

        // Find the eval function from the conversion lattice
        TypeEdge newEdge = TypeLattice.getTransformLattice().getEdgeFromTypes(sourceType, targetType, null);
        if (newEdge != null) {
            typeConversionExpr.setOpcode(newEdge.getOpcode());

            if (!newEdge.isSafe() && !isMultiReturn) {
                BLangExceptionHelper.throwSemanticError(typeConversionExpr, SemanticErrors.UNSAFE_CONVERSION_ATTEMPT,
                        sourceType, targetType);
            }

            if (!isMultiReturn) {
                typeConversionExpr.setTypes(new BType[]{targetType});
                return;
            }

        } else if (sourceType == targetType) {
            typeConversionExpr.setOpcode(InstructionCodes.NOP);
            if (!isMultiReturn) {
                typeConversionExpr.setTypes(new BType[]{targetType});
                return;
            }
        } else {
            TypeEdge castEdge = TypeLattice.getExplicitCastLattice().getEdgeFromTypes(sourceType, targetType, null);
            if (castEdge != null) {
                throw BLangExceptionHelper.getSemanticError(typeConversionExpr.getNodeLocation(),
                        SemanticErrors.CANNOT_CONVERT_WITH_SUGGESTION, sourceType, targetType);
            }
            throw BLangExceptionHelper.getSemanticError(typeConversionExpr.getNodeLocation(),
                    SemanticErrors.INCOMPATIBLE_TYPES_CANNOT_CONVERT, sourceType, targetType);
        }

        // If this is a multi-value return conversion expression, set the return types.
        BLangSymbol error = currentScope.resolve(new SymbolName(BALLERINA_CONVERSION_ERROR, ERRORS_PACKAGE));
        if (error == null || !(error instanceof StructDef)) {
            BLangExceptionHelper.throwSemanticError(typeConversionExpr,
                    SemanticErrors.CANNOT_RESOLVE_STRUCT, ERRORS_PACKAGE, BALLERINA_CAST_ERROR);
        }
        typeConversionExpr.setTypes(new BType[]{targetType, (BType) error});
    }

    @Override
    public void visit(NullLiteral nullLiteral) {
        nullLiteral.setType(BTypes.typeNull);
    }

    @Override
    public void visit(LambdaExpression lambdaExpr) {
    }

    @Override
    public void visit(StringTemplateLiteral stringTemplateLiteral) {
        Expression[] items = stringTemplateLiteral.getArgExprs();
        Expression concatExpr;
        if (items.length == 1) {
            concatExpr = items[0];
        } else {
            concatExpr = items[0];
            for (int i = 1; i < items.length; i++) {
                Expression currentItem = items[i];
                concatExpr = new AddExpression(currentItem.getNodeLocation(), currentItem.getWhiteSpaceDescriptor(),
                                               concatExpr, currentItem);
            }
        }
        concatExpr.accept(this);
        concatExpr.setType(BTypes.typeString);
        stringTemplateLiteral.setConcatExpr(concatExpr);
        stringTemplateLiteral.setType(BTypes.typeString);
    }

    @Override
    public void visit(NamespaceDeclarationStmt namespaceDeclarationStmt) {
        namespaceDeclarationStmt.getNamespaceDclr().accept(this);
    }

    @Override
    public void visit(NamespaceDeclaration namespaceDclr) {
        if (namespaceDclr.getNamespaceUri().isEmpty() && !namespaceDclr.getPrefix().isEmpty()) {
            BLangExceptionHelper.throwSemanticError(namespaceDclr, SemanticErrors.INVALID_NAMESPACE_DECLARATION,
                    namespaceDclr.getPrefix());
        }

        // Check whether this namespace is already defined, if not define it.
        NamespaceSymbolName nsSymbolName = new NamespaceSymbolName(namespaceDclr.getPrefix());

        BLangSymbol nsSymbol = currentScope.resolve(nsSymbolName);
        if (nsSymbol != null && nsSymbol.getSymbolScope().getScopeName() == currentScope.getScopeName()) {
            BLangExceptionHelper.throwSemanticError(namespaceDclr, SemanticErrors.REDECLARED_SYMBOL,
                    namespaceDclr.getPrefix());
        }

        currentScope.define(nsSymbolName, namespaceDclr);
    }

    @Override
    public void visit(XMLLiteral xmlLiteral) {
    }

    @Override
    public void visit(XMLElementLiteral xmlElementLiteral) {
        Expression startTagName = xmlElementLiteral.getStartTagName();
        Map<String, Expression> namespaces;
        XMLElementLiteral parent = xmlElementLiteral.getParent();
        if (parent == null) {
            namespaces = getNamespaceInScope(xmlElementLiteral.getNodeLocation());
        } else {
            namespaces = parent.getNamespaces();
            xmlElementLiteral.setDefaultNamespaceUri(parent.getDefaultNamespaceUri());
        }
        xmlElementLiteral.setNamespaces(namespaces);

        // add the inline declared namespaces to the namespace map
        List<KeyValueExpr> attributes = xmlElementLiteral.getAttributes();
        Iterator<KeyValueExpr> attrItr = attributes.iterator();
        while (attrItr.hasNext()) {
            KeyValueExpr attribute = attrItr.next();
            Expression attrNameExpr = attribute.getKeyExpr();
            if (!(attrNameExpr instanceof XMLQNameExpr)) {
                continue;
            }

            Expression attrValueExpr = attribute.getValueExpr();
            XMLQNameExpr xmlQNameRefExpr = (XMLQNameExpr) attrNameExpr;
            if (xmlQNameRefExpr.getPrefix().equals(XMLConstants.XMLNS_ATTRIBUTE)) {
                attrValueExpr.accept(this);

                if (attrValueExpr instanceof BasicLiteral
                        && ((BasicLiteral) attrValueExpr).getBValue().stringValue().isEmpty()) {
                    BLangExceptionHelper.throwSemanticError(attribute, SemanticErrors.INVALID_NAMESPACE_DECLARATION,
                            xmlQNameRefExpr.getLocalname());
                }

                namespaces.put(xmlQNameRefExpr.getLocalname(), attrValueExpr);
                attrItr.remove();
                continue;
            }

            // if the default namesapce is declared inline, then override default namepsace defined at the
            // parent scope level
            if (xmlQNameRefExpr.getLocalname().equals(XMLConstants.XMLNS_ATTRIBUTE)) {
                attrValueExpr.accept(this);
                xmlElementLiteral.setDefaultNamespaceUri(attrValueExpr);
                attrItr.remove();
            }
        }

        if (xmlElementLiteral.getDefaultNamespaceUri() == null) {
            BasicLiteral defaultnsUriLiteral = new BasicLiteral(xmlElementLiteral.getNodeLocation(), null,
                    new SimpleTypeName(TypeConstants.STRING_TNAME), new BString(XMLConstants.XMLNS_ATTRIBUTE_NS_URI));
            defaultnsUriLiteral.setType(BTypes.typeString);
            defaultnsUriLiteral.accept(this);
            xmlElementLiteral.setDefaultNamespaceUri(defaultnsUriLiteral);
        }

        validateXMLLiteralAttributes(attributes, namespaces);

        // Validate start tag
        if (startTagName instanceof XMLQNameExpr) {
            validateXMLQname((XMLQNameExpr) startTagName, namespaces, xmlElementLiteral.getDefaultNamespaceUri());
        } else {
            startTagName.accept(this);
        }

        if (startTagName.getType() != BTypes.typeString) {
            // Implicit cast from right to left
            startTagName = createImplicitStringConversionExpr(startTagName, startTagName.getType());
            xmlElementLiteral.setStartTagName(startTagName);
        }

        // Validate the ending tag of the XML element
        validateXMLLiteralEndTag(xmlElementLiteral, xmlElementLiteral.getDefaultNamespaceUri());

        // Visit children
        XMLSequenceLiteral children = xmlElementLiteral.getContent();
        if (children != null) {
            children.accept(this);
        }
    }

    @Override
    public void visit(XMLCommentLiteral xmlComment) {
        Expression contentExpr = xmlComment.getContent();
        if (contentExpr == null) {
            return;
        }

        contentExpr.accept(this);
        if (contentExpr.getType() != BTypes.typeString) {
            contentExpr = createImplicitStringConversionExpr(contentExpr, contentExpr.getType());
            xmlComment.setContent(contentExpr);
        }
    }

    @Override
    public void visit(XMLTextLiteral xmlText) {
        Expression contentExpr = xmlText.getContent();
        if (contentExpr == null) {
            return;
        }
        contentExpr.accept(this);
    }

    @Override
    public void visit(XMLSequenceLiteral xmlSequence) {
        Expression[] items = xmlSequence.getItems();
        List<Expression> newItems = new ArrayList<Expression>();

        // Consecutive non-xml type items are converted to string, and combined together using binary add expressions.
        Expression addExpr = null;
        for (int i = 0; i < items.length; i++) {
            Expression currentItem = items[i];
            currentItem.accept(this);

            if (xmlSequence.hasParent() && currentItem.getType() == BTypes.typeXML) {
                if (addExpr != null) {
                    newItems.add(addExpr);
                    addExpr = null;
                }
                newItems.add(currentItem);
                continue;
            }

            if (currentItem.getType() != BTypes.typeString) {
                Expression castExpr = getImplicitConversionExpr(currentItem, currentItem.getType(), BTypes.typeString);

                if (castExpr == null) {
                    if (xmlSequence.hasParent()) {
                        BLangExceptionHelper.throwSemanticError(currentItem,
                                SemanticErrors.INCOMPATIBLE_TYPES_IN_XML_TEMPLATE, currentItem.getType());
                    }
                    BLangExceptionHelper.throwSemanticError(currentItem, SemanticErrors.INCOMPATIBLE_TYPES,
                            BTypes.typeString, currentItem.getType());
                }

                currentItem = castExpr;
            }

            if (addExpr == null) {
                addExpr = currentItem;
                continue;
            }

            if (addExpr.getType() == BTypes.typeString) {
                addExpr = new AddExpression(currentItem.getNodeLocation(), currentItem.getWhiteSpaceDescriptor(),
                        addExpr, currentItem);
            } else {
                newItems.add(addExpr);
                addExpr = currentItem;
            }
            addExpr.setType(BTypes.typeString);
        }

        if (addExpr != null) {
            newItems.add(addExpr);
        }

        // Replace the existing items with the new reduced items
        items = newItems.toArray(new Expression[newItems.size()]);
        xmlSequence.setItems(items);

        // Create and set XML concatenation expression using all the items in the sequence
        xmlSequence.setConcatExpr(getXMLConcatExpression(items));
    }

    @Override
    public void visit(XMLPILiteral xmlPI) {
        Expression target = xmlPI.getTarget();
        target.accept(this);

        if (target.getType() != BTypes.typeString) {
            target = createImplicitStringConversionExpr(target, target.getType());
            xmlPI.setTarget(target);
        }

        Expression data = xmlPI.getData();
        if (data == null) {
            return;
        }

        data.accept(this);
        if (data.getType() != BTypes.typeString) {
            data = createImplicitStringConversionExpr(data, data.getType());
            xmlPI.setData(data);
        }
    }

    // Private methods.

    private void openScope(SymbolScope symbolScope) {
        currentScope = symbolScope;
    }

    private void closeScope() {
        currentScope = currentScope.getEnclosingScope();
    }

    private void visitBinaryExpr(BinaryExpression expr) {
        visitSingleValueExpr(expr.getLExpr());
        visitSingleValueExpr(expr.getRExpr());
    }

    private void visitSingleValueExpr(Expression expr) {
        expr.accept(this);
        if (expr.isMultiReturnExpr()) {
            FunctionInvocationExpr funcIExpr = (FunctionInvocationExpr) expr;
            String nameWithPkgName = (funcIExpr.getPackageName() != null) ? funcIExpr.getPackageName()
                    + ":" + funcIExpr.getName() : funcIExpr.getName();
            BLangExceptionHelper.throwSemanticError(expr, SemanticErrors.MULTIPLE_VALUE_IN_SINGLE_VALUE_CONTEXT,
                    nameWithPkgName);
        }
    }

    private void validateBinaryExprTypeForIntFloat(BinaryExpression binaryExpr, BType binaryExprType) {
        if (binaryExprType != BTypes.typeInt && binaryExprType != BTypes.typeFloat) {
            throwInvalidBinaryOpError(binaryExpr);
        }
    }

    private BType verifyBinaryArithmeticExprType(BinaryArithmeticExpression binaryArithmeticExpr) {
        visitBinaryExpr(binaryArithmeticExpr);
        BType type = verifyBinaryExprType(binaryArithmeticExpr);
        binaryArithmeticExpr.setType(type);
        return type;
    }

    private BType verifyBinaryCompareExprType(BinaryExpression binaryExpression) {
        visitBinaryExpr(binaryExpression);
        BType type = verifyBinaryExprType(binaryExpression);
        binaryExpression.setType(BTypes.typeBoolean);
        return type;
    }

    private void verifyBinaryEqualityExprType(BinaryExpression binaryExpr) {
        visitBinaryExpr(binaryExpr);
        BType rType = binaryExpr.getRExpr().getType();
        BType lType = binaryExpr.getLExpr().getType();
        BType type;

        if (rType == BTypes.typeNull) {
            if (BTypes.isValueType(lType)) {
                BLangExceptionHelper.throwSemanticError(binaryExpr,
                        SemanticErrors.INVALID_OPERATION_INCOMPATIBLE_TYPES, lType, rType);
            }
            type = rType;
        } else if (lType == BTypes.typeNull) {
            if (BTypes.isValueType(rType)) {
                BLangExceptionHelper.throwSemanticError(binaryExpr,
                        SemanticErrors.INVALID_OPERATION_INCOMPATIBLE_TYPES, lType, rType);
            }
            type = lType;
        } else {
            type = verifyBinaryExprType(binaryExpr);
        }

        binaryExpr.setType(BTypes.typeBoolean);

        if (type != BTypes.typeInt &&
                type != BTypes.typeFloat &&
                type != BTypes.typeBoolean &&
                type != BTypes.typeString &&
                type != BTypes.typeNull &&
                type != BTypes.typeType) {
            throwInvalidBinaryOpError(binaryExpr);
        }
    }

    private BType verifyBinaryExprType(BinaryExpression binaryExpr) {
        Expression rExpr = binaryExpr.getRExpr();
        Expression lExpr = binaryExpr.getLExpr();
        BType rType = rExpr.getType();
        BType lType = lExpr.getType();

        if (rType.equals(lType)) {
            return rType;
        }

        if ((rType.equals(BTypes.typeString) || lType.equals(BTypes.typeString))
                && !(binaryExpr.getOperator().equals(Operator.ADD))) {
            throw getInvalidBinaryOpError(binaryExpr);
        }

        if ((rType.equals(BTypes.typeString))) {
            // Implicit cast from left to right
            Expression newExpr = createConversionExpr(binaryExpr, lExpr, lType, rType);
            binaryExpr.setLExpr(newExpr);
            return rType;
        } else if (lType.equals(BTypes.typeString)) {
            // Implicit cast from right to left
            Expression newExpr = createConversionExpr(binaryExpr, rExpr, rType, lType);
            binaryExpr.setRExpr(newExpr);
            return lType;
        }

        if (rType.equals(BTypes.typeInt) && lType.equals(BTypes.typeFloat)) {
            Expression newExpr = createConversionExpr(binaryExpr, rExpr, rType, lType);
            binaryExpr.setRExpr(newExpr);
            return lType;
        }

        if (lType.equals(BTypes.typeInt) && rType.equals(BTypes.typeFloat)) {
            Expression newExpr = createConversionExpr(binaryExpr, lExpr, lType, rType);
            binaryExpr.setLExpr(newExpr);
            return rType;
        }
        throw getInvalidBinaryOpError(binaryExpr);
    }

    private Expression createConversionExpr(BinaryExpression binaryExpr, Expression sExpr, BType sType, BType tType) {
        Expression conversionExpr = getImplicitConversionExpr(sExpr, sType, tType);
        if (conversionExpr != null) {
            return conversionExpr;
        }

        throw getInvalidBinaryOpError(binaryExpr);
    }

    private Expression getImplicitConversionExpr(Expression sExpr, BType sType, BType tType) {
        TypeEdge newEdge;
        newEdge = TypeLattice.getTransformLattice().getEdgeFromTypes(sType, tType, null);
        if (newEdge != null) {
            TypeConversionExpr newExpr =
                    new TypeConversionExpr(sExpr.getNodeLocation(), sExpr.getWhiteSpaceDescriptor(), sExpr, tType);
            newExpr.setOpcode(newEdge.getOpcode());
            newExpr.accept(this);
            return newExpr;
        }

        return null;
    }

    private void visitBinaryLogicalExpr(BinaryLogicalExpression expr) {
        visitBinaryExpr(expr);

        Expression rExpr = expr.getRExpr();
        Expression lExpr = expr.getLExpr();

        if (lExpr.getType() == BTypes.typeBoolean && rExpr.getType() == BTypes.typeBoolean) {
            expr.setType(BTypes.typeBoolean);
        } else {
            throwInvalidBinaryOpError(expr);
        }
    }

    private void checkForConstAssignment(AssignStmt assignStmt, Expression lExpr) {
        if (lExpr instanceof SimpleVarRefExpr &&
                ((SimpleVarRefExpr) lExpr).getVariableDef().getKind() == VariableDef.Kind.CONSTANT) {
            BLangExceptionHelper.throwSemanticError(assignStmt, SemanticErrors.CANNOT_ASSIGN_VALUE_CONSTANT,
                    ((SimpleVarRefExpr) lExpr).getSymbolName());
        }
    }

    private void checkForMultiAssignmentErrors(AssignStmt assignStmt, Expression[] lExprs,
                                               CallableUnitInvocationExpr rExpr) {
        BType[] returnTypes = rExpr.getTypes();
        if (lExprs.length != returnTypes.length) {
            BLangExceptionHelper.throwSemanticError(assignStmt,
                    SemanticErrors.ASSIGNMENT_COUNT_MISMATCH, lExprs.length, returnTypes.length);
        }

        //cannot assign string to b (type int) in multiple assignment

        for (int i = 0; i < lExprs.length; i++) {
            Expression lExpr = lExprs[i];
            if (lExpr instanceof SimpleVarRefExpr) {
                String varName = ((SimpleVarRefExpr) lExpr).getVarName();
                if ("_".equals(varName)) {
                    continue;
                }
            }

            BType lhsType = lExprs[i].getType();
            BType rhsType = returnTypes[i];

            // Check whether the right-hand type can be assigned to the left-hand type.
            if (isAssignableTo(lhsType, rhsType)) {
                continue;
            }

            // TODO Check whether an implicit cast is possible
            // This requires a tree rewrite. Off the top of my head the results of function or action invocation
            // should be stored in temporary variables with matching types. Then these temporary variables can be
            // assigned to left-hand side expressions one by one.

            BLangExceptionHelper.throwSemanticError(assignStmt,
                    SemanticErrors.INCOMPATIBLE_ASSIGNMENT, rhsType, lExpr.getType());
        }
    }

    private void checkForMultiValuedCastingErrors(AssignStmt assignStmt, Expression[] lExprs,
                                                  ExecutableMultiReturnExpr rExpr) {
        BType[] returnTypes = rExpr.getTypes();
        if (lExprs.length != returnTypes.length) {
            BLangExceptionHelper.throwSemanticError(assignStmt, SemanticErrors.ASSIGNMENT_COUNT_MISMATCH,
                    lExprs.length, returnTypes.length);
        }

        for (int i = 0; i < lExprs.length; i++) {
            Expression lExpr = lExprs[i];
            BType returnType = returnTypes[i];
            if (lExpr instanceof SimpleVarRefExpr && ((SimpleVarRefExpr) lExpr).getVarName().equals("_")) {
                continue;
            }

            if ((lExpr.getType() != BTypes.typeAny) && (!lExpr.getType().equals(returnType))) {
                BLangExceptionHelper.throwSemanticError(assignStmt,
                        SemanticErrors.INCOMPATIBLE_TYPES, returnType, lExpr.getType());
            }
        }
    }

    private void visitLExprsOfAssignment(AssignStmt assignStmt, Expression[] lExprs) {
        // Handle special case for assignment statement declared with var
        if (assignStmt.isDeclaredWithVar()) {
            // This set data structure is used to check for repeated variable names in the assignment statement
            Set<String> varNameSet = new HashSet<>();
            int declaredVarCount = 0;
            for (Expression expr : lExprs) {
                if (!(expr instanceof SimpleVarRefExpr)) {
                    throw BLangExceptionHelper.getSemanticError(assignStmt.getNodeLocation(),
                            SemanticErrors.INVALID_VAR_ASSIGNMENT);
                }

                SimpleVarRefExpr refExpr = (SimpleVarRefExpr) expr;
                String varName = refExpr.getVarName();
                // Continue to next iteration if variable symbol is underscore '_' == ignore
                if (varName.equals("_")) {
                    declaredVarCount++;
                    continue;
                }

                if (!varNameSet.add(varName)) {
                    BLangExceptionHelper.throwSemanticError(assignStmt,
                            SemanticErrors.VAR_IS_REPEATED_ON_LEFT_SIDE_ASSIGNMENT, varName);
                }

                Identifier identifier = new Identifier(varName);
                SymbolName symbolName = new SymbolName(identifier.getName());
                SimpleVariableDef variableDef = new SimpleVariableDef(refExpr.getNodeLocation(),
                        refExpr.getWhiteSpaceDescriptor(), identifier,
                        null, symbolName, currentScope);
                variableDef.setKind(VariableDef.Kind.LOCAL_VAR);

                // Check whether this variable is already defined, if not define it.
                SymbolName varDefSymName = new SymbolName(variableDef.getName(), currentPkg);
                BLangSymbol varSymbol = currentScope.resolve(varDefSymName);
                if (varSymbol != null && varSymbol.getSymbolScope().getScopeName() == currentScope.getScopeName()) {
                    declaredVarCount++;
                    continue;
                }
                currentScope.define(varDefSymName, variableDef);
            }

            if (declaredVarCount == lExprs.length) {
                throw new SemanticException(BLangExceptionHelper.constructSemanticError(
                        assignStmt.getNodeLocation(), SemanticErrors.NO_NEW_VARIABLES_VAR_ASSIGNMENT));
            }
        }

        int ignoredVarCount = 0;
        for (Expression lExpr : lExprs) {
            if (lExpr instanceof SimpleVarRefExpr && ((SimpleVarRefExpr) lExpr).getVarName().equals("_")) {
                ignoredVarCount++;
                continue;
            }

            // First mark all left side ArrayMapAccessExpr. This is to skip some processing which is applicable only
            // for right side expressions.
            ((VariableReferenceExpr) lExpr).setLHSExpr(true);
            lExpr.accept(this);

            // Check whether someone is trying to change the values of a constant
            checkForConstAssignment(assignStmt, lExpr);
        }
        if (ignoredVarCount == lExprs.length) {
            throw new SemanticException(BLangExceptionHelper.constructSemanticError(
                    assignStmt.getNodeLocation(), SemanticErrors.IGNORED_ASSIGNMENT));
        }
    }

    private void linkFunction(FunctionInvocationExpr funcIExpr) {
        String pkgPath = funcIExpr.getPackagePath();

        Expression[] exprs = funcIExpr.getArgExprs();
        BType[] paramTypes = new BType[exprs.length];
        for (int i = 0; i < exprs.length; i++) {
            paramTypes[i] = exprs[i].getType();
        }

        FunctionSymbolName symbolName = LangModelUtils.getFuncSymNameWithParams(funcIExpr.getName(),
                pkgPath, paramTypes);
        BLangSymbol functionSymbol = currentScope.resolve(symbolName);

        if (functionSymbol instanceof SimpleVariableDef
                && ((SimpleVariableDef) functionSymbol).getType() instanceof BFunctionType) {
            SimpleVariableDef variableDef = (SimpleVariableDef) functionSymbol;
            matchAndUpdateFunctionPointsArgs(funcIExpr, symbolName, (BFunctionType) (variableDef).getType());
            // Link at runtime.
            funcIExpr.setFunctionPointerInvocation(true);
            funcIExpr.setFunctionPointerVariableDef(variableDef);
            return;
        }

        functionSymbol = matchAndUpdateArguments(funcIExpr, symbolName, functionSymbol);

        if (functionSymbol == null) {
            String funcName = (funcIExpr.getPackageName() != null) ? funcIExpr.getPackageName() + ":" +
                    funcIExpr.getName() : funcIExpr.getName();
            BLangExceptionHelper.throwSemanticError(funcIExpr, SemanticErrors.UNDEFINED_FUNCTION, funcName);
            return;
        }

        Function function;
        if (functionSymbol.isNative()) {
            functionSymbol = ((BallerinaFunction) functionSymbol).getNativeFunction();
            NativeUnit nativeUnit = ((NativeUnitProxy) functionSymbol).load();
            // Loading return parameter types of this native function
            SimpleTypeName[] returnParamTypeNames = nativeUnit.getReturnParamTypeNames();
            BType[] returnTypes = new BType[returnParamTypeNames.length];
            for (int i = 0; i < returnParamTypeNames.length; i++) {
                SimpleTypeName typeName = returnParamTypeNames[i];
                BType bType = BTypes.resolveType(typeName, currentScope, funcIExpr.getNodeLocation());
                returnTypes[i] = bType;
            }

            if (!(nativeUnit instanceof Function)) {
                BLangExceptionHelper.throwSemanticError(funcIExpr, SemanticErrors.INCOMPATIBLE_TYPES_UNKNOWN_FOUND,
                        symbolName);
            }
            function = (Function) nativeUnit;
            function.setReturnParamTypes(returnTypes);

        } else {
            if (!(functionSymbol instanceof Function)) {
                BLangExceptionHelper.throwSemanticError(funcIExpr, SemanticErrors.INCOMPATIBLE_TYPES_UNKNOWN_FOUND,
                        symbolName);
                return;
            }
            function = (Function) functionSymbol;
        }

        // Link the function with the function invocation expression
        funcIExpr.setCallableUnit(function);
    }

    private void linkAction(ActionInvocationExpr actionIExpr) {
        String pkgPath = actionIExpr.getPackagePath();
        String connectorName = actionIExpr.getConnectorName();

        // First look for the connectors
        SymbolName connectorSymbolName = new SymbolName(connectorName, pkgPath);
        BLangSymbol connectorSymbol = currentScope.resolve(connectorSymbolName);
        if (connectorSymbol == null) {
            String connectorWithPkgName = (actionIExpr.getPackageName() != null) ? actionIExpr.getPackageName() +
                    ":" + actionIExpr.getConnectorName() : actionIExpr.getConnectorName();
            BLangExceptionHelper.throwSemanticError(actionIExpr, SemanticErrors.UNDEFINED_CONNECTOR,
                    connectorWithPkgName);
            return;
        }

        Expression[] exprs = actionIExpr.getArgExprs();
        BType[] paramTypes = new BType[exprs.length];
        for (int i = 0; i < exprs.length; i++) {
            paramTypes[i] = exprs[i].getType();
        }

        // When getting the action symbol name, Package name for the action is set to null, since the action is 
        // registered under connector, and connecter contains the package
        ActionSymbolName actionSymbolName = LangModelUtils.getActionSymName(actionIExpr.getName(),
                actionIExpr.getPackagePath(), actionIExpr.getConnectorName(), paramTypes);

        // Now check whether there is a matching action
        BLangSymbol actionSymbol = null;
        if (connectorSymbol instanceof BallerinaConnectorDef) {
            actionSymbol = ((BallerinaConnectorDef) connectorSymbol).resolveMembers(actionSymbolName);
        } else {
            BLangExceptionHelper.throwSemanticError(actionIExpr, SemanticErrors.INCOMPATIBLE_TYPES_CONNECTOR_EXPECTED,
                    connectorSymbolName);
        }

        actionSymbol = matchAndUpdateArguments(actionIExpr, actionSymbolName, actionSymbol);

        if ((actionSymbol instanceof BallerinaAction) && (actionSymbol.isNative())) {
            actionSymbol = ((BallerinaAction) actionSymbol).getNativeAction();
        }

        if (actionSymbol == null) {
            BLangExceptionHelper.throwSemanticError(actionIExpr, SemanticErrors.UNDEFINED_ACTION,
                    actionIExpr.getName(), connectorSymbol.getSymbolName());
        }

        // Load native action
        Action action = null;
        if (actionSymbol instanceof NativeUnitProxy) {
            // Loading return parameter types of this native function
            NativeUnit nativeUnit = ((NativeUnitProxy) actionSymbol).load();
            SimpleTypeName[] returnParamTypeNames = nativeUnit.getReturnParamTypeNames();
            BType[] returnTypes = new BType[returnParamTypeNames.length];
            for (int i = 0; i < returnParamTypeNames.length; i++) {
                SimpleTypeName typeName = returnParamTypeNames[i];
                BType bType = BTypes.resolveType(typeName, currentScope, actionIExpr.getNodeLocation());
                returnTypes[i] = bType;
            }

            if (!(nativeUnit instanceof Action)) {
                BLangExceptionHelper.throwSemanticError(actionIExpr, SemanticErrors.INCOMPATIBLE_TYPES_UNKNOWN_FOUND,
                        actionSymbolName);
            }
            action = (Action) nativeUnit;
            action.setReturnParamTypes(returnTypes);

        } else if (actionSymbol instanceof Action) {
            action = (Action) actionSymbol;
        } else {
            BLangExceptionHelper.throwSemanticError(actionIExpr, SemanticErrors.INCOMPATIBLE_TYPES_UNKNOWN_FOUND,
                    actionSymbolName);
        }

        // Link the action with the action invocation expression
        actionIExpr.setCallableUnit(action);
    }

    /**
     * Helper method to match the callable unit with invocation (check whether parameters map, do cast if applicable).
     *
     * @param callableIExpr  invocation expression
     * @param symbolName     callable symbol name
     * @param callableSymbol matching symbol
     * @return callableSymbol  matching symbol
     */
    private BLangSymbol matchAndUpdateArguments(AbstractExpression callableIExpr,
                                                CallableUnitSymbolName symbolName, BLangSymbol callableSymbol) {
        if (callableSymbol == null) {
            return null;
        }

        Expression[] argExprs = ((CallableUnitInvocationExpr) callableIExpr).getArgExprs();
        Expression[] updatedArgExprs = new Expression[argExprs.length];

        CallableUnitSymbolName funcSymName = (CallableUnitSymbolName) callableSymbol.getSymbolName();
        if (!funcSymName.isNameAndParamCountMatch(symbolName)) {
            return null;
        }

        boolean implicitCastPossible = true;

        if (callableSymbol instanceof NativeUnitProxy) {
            NativeUnit nativeUnit = ((NativeUnitProxy) callableSymbol).load();
            for (int i = 0; i < argExprs.length; i++) {
                Expression argExpr = argExprs[i];
                updatedArgExprs[i] = argExpr;
                SimpleTypeName simpleTypeName = nativeUnit.getArgumentTypeNames()[i];
                BType lhsType = BTypes.resolveType(simpleTypeName, currentScope, callableIExpr.getNodeLocation());

                AssignabilityResult result = performAssignabilityCheck(lhsType, argExpr);
                if (result.expression != null) {
                    updatedArgExprs[i] = result.expression;
                } else if (!result.assignable) {
                    // TODO do we need to throw an error here?
                    implicitCastPossible = false;
                    break;
                }
            }
        } else {
            for (int i = 0; i < argExprs.length; i++) {
                Expression argExpr = argExprs[i];
                updatedArgExprs[i] = argExpr;
                BType lhsType = ((CallableUnit) callableSymbol).getParameterDefs()[i].getType();

                AssignabilityResult result = performAssignabilityCheck(lhsType, argExpr);
                if (result.expression != null) {
                    updatedArgExprs[i] = result.expression;
                } else if (!result.assignable) {
                    // TODO do we need to throw an error here?
                    implicitCastPossible = false;
                    break;
                }
            }
        }

        if (!implicitCastPossible) {
            return null;
        }

        for (int i = 0; i < updatedArgExprs.length; i++) {
            ((CallableUnitInvocationExpr) callableIExpr).getArgExprs()[i] = updatedArgExprs[i];
        }
        return callableSymbol;
    }

    private void matchAndUpdateFunctionPointsArgs(FunctionInvocationExpr funcIExpr,
                                                  CallableUnitSymbolName symbolName, BFunctionType bFunctionType) {
        if (symbolName.getNoOfParameters() != bFunctionType.getParameterType().length) {
            BLangExceptionHelper.throwSemanticError(funcIExpr, SemanticErrors.INCORRECT_FUNCTION_ARGUMENTS,
                    funcIExpr.getName());
        }
        Expression[] argExprs = funcIExpr.getArgExprs();
        Expression[] updatedArgExprs = new Expression[argExprs.length];
        for (int i = 0; i < argExprs.length; i++) {
            Expression argExpr = argExprs[i];
            updatedArgExprs[i] = argExpr;
            BType lhsType = bFunctionType.getParameterType()[i];

            AssignabilityResult result = performAssignabilityCheck(lhsType, argExpr);
            if (result.expression != null) {
                updatedArgExprs[i] = result.expression;
            } else if (!result.assignable) {
                BLangExceptionHelper.throwSemanticError(funcIExpr, SemanticErrors.INCORRECT_FUNCTION_ARGUMENTS,
                        funcIExpr.getName());
            }
        }
        for (int i = 0; i < updatedArgExprs.length; i++) {
            funcIExpr.getArgExprs()[i] = updatedArgExprs[i];
        }
    }

    private void linkWorker(WorkerInvocationStmt workerInvocationStmt) {
        String workerName = workerInvocationStmt.getCallableUnitName();
        SymbolName workerSymbolName = new SymbolName(workerName);
        Worker worker = (Worker) currentScope.resolve(workerSymbolName);
        if (worker == null) {
            throw new LinkerException(workerInvocationStmt.getNodeLocation().getFileName() + ":" +
                    workerInvocationStmt.getNodeLocation().getLineNumber() +
                    ": undefined worker '" + workerInvocationStmt.getCallableUnitName() + "'");
        }
        workerInvocationStmt.setCallableUnit(worker);
    }

    private void throwInvalidBinaryOpError(BinaryExpression binaryExpr) {
        BType lExprType = binaryExpr.getLExpr().getType();
        BType rExprType = binaryExpr.getRExpr().getType();

        if (lExprType == rExprType) {
            BLangExceptionHelper.throwSemanticError(binaryExpr,
                    SemanticErrors.INVALID_OPERATION_OPERATOR_NOT_DEFINED, binaryExpr.getOperator(), lExprType);
        } else {
            BLangExceptionHelper.throwSemanticError(binaryExpr,
                    SemanticErrors.INVALID_OPERATION_INCOMPATIBLE_TYPES, lExprType, rExprType);
        }
    }

    private SemanticException getInvalidBinaryOpError(BinaryExpression binaryExpr) {
        BType lExprType = binaryExpr.getLExpr().getType();
        BType rExprType = binaryExpr.getRExpr().getType();

        if (lExprType == rExprType) {
            return BLangExceptionHelper.getSemanticError(binaryExpr.getNodeLocation(),
                    SemanticErrors.INVALID_OPERATION_OPERATOR_NOT_DEFINED, binaryExpr.getOperator(), lExprType);
        } else {
            return BLangExceptionHelper.getSemanticError(binaryExpr.getNodeLocation(),
                    SemanticErrors.INVALID_OPERATION_INCOMPATIBLE_TYPES, lExprType, rExprType);
        }
    }

    private void throwInvalidUnaryOpError(UnaryExpression unaryExpr) {
        BType rExprType = unaryExpr.getRExpr().getType();
        BLangExceptionHelper.throwSemanticError(unaryExpr,
                SemanticErrors.INVALID_OPERATION_OPERATOR_NOT_DEFINED, unaryExpr.getOperator(), rExprType);
    }

    private TypeCastExpression checkWideningPossible(BType lhsType, Expression rhsExpr) {
        TypeCastExpression typeCastExpr = null;
        BType rhsType = rhsExpr.getType();

        TypeEdge typeEdge = TypeLattice.getImplicitCastLattice().getEdgeFromTypes(rhsType, lhsType, null);
        if (typeEdge != null) {
            typeCastExpr = new TypeCastExpression(rhsExpr.getNodeLocation(),
                    rhsExpr.getWhiteSpaceDescriptor(), rhsExpr, lhsType);
            typeCastExpr.setOpcode(typeEdge.getOpcode());
        }
        return typeCastExpr;
    }

    private void defineWorkers(Worker[] workers, CallableUnit callableUnit) {
        for (Worker worker : workers) {

            SymbolName symbolName = new SymbolName(worker.getName(), null);
            worker.setSymbolName(symbolName);

            BLangSymbol workerSymbol = callableUnit.getSymbolScope().resolve(symbolName);
            if (workerSymbol != null) {
                BLangExceptionHelper.throwSemanticError(worker,
                        SemanticErrors.REDECLARED_SYMBOL, worker.getName());
            }
            callableUnit.getSymbolScope().define(symbolName, worker);
        }
    }

    private void defineFunctions(Function[] functions) {
        for (Function function : functions) {
            // Resolve input parameters
            ParameterDef[] paramDefArray = function.getParameterDefs();
            BType[] paramTypes = new BType[paramDefArray.length];
            for (int i = 0; i < paramDefArray.length; i++) {
                ParameterDef paramDef = paramDefArray[i];
                BType bType = BTypes.resolveType(paramDef.getTypeName(), currentScope, paramDef.getNodeLocation());
                paramDef.setType(bType);
                paramTypes[i] = bType;
            }

            function.setParameterTypes(paramTypes);
            FunctionSymbolName symbolName = LangModelUtils.getFuncSymNameWithParams(function.getName(),
                    function.getPackagePath(), paramTypes);
            function.setSymbolName(symbolName);

            BLangSymbol functionSymbol = currentScope.resolve(symbolName);

            if (!function.isNative() && functionSymbol != null) {
                BLangExceptionHelper.throwSemanticError(function,
                        SemanticErrors.REDECLARED_SYMBOL, function.getName());
            }

            if (function.isNative() && functionSymbol == null) {
                functionSymbol = nativeScope.resolve(symbolName);
                if (functionSymbol == null) {
                    BLangExceptionHelper.throwSemanticError(function,
                            SemanticErrors.UNDEFINED_FUNCTION, function.getName());
                }
                if (function instanceof BallerinaFunction) {
                    ((BallerinaFunction) function).setNativeFunction((NativeUnitProxy) functionSymbol);
                }
            }

            currentScope.define(symbolName, function);

            // Resolve return parameters
            ParameterDef[] returnParameters = function.getReturnParameters();
            BType[] returnTypes = new BType[returnParameters.length];
            for (int i = 0; i < returnParameters.length; i++) {
                ParameterDef paramDef = returnParameters[i];
                BType bType = BTypes.resolveType(paramDef.getTypeName(), currentScope, paramDef.getNodeLocation());
                paramDef.setType(bType);
                returnTypes[i] = bType;
            }
            function.setReturnParamTypes(returnTypes);

            if (function.getWorkers().length > 0) {
                defineWorkers(function.getWorkers(), function);
            }
        }
    }

    private void defineConnectors(BallerinaConnectorDef[] connectorDefArray) {
        for (BallerinaConnectorDef connectorDef : connectorDefArray) {
            String connectorName = connectorDef.getName();

            // Define ConnectorDef Symbol in the package scope..
            SymbolName connectorSymbolName = new SymbolName(connectorName, connectorDef.getPackagePath());
            BLangSymbol connectorSymbol = currentScope.resolve(connectorSymbolName);
            if (connectorSymbol != null) {
                BLangExceptionHelper.throwSemanticError(connectorDef,
                        SemanticErrors.REDECLARED_SYMBOL, connectorName);
            }
            currentScope.define(connectorSymbolName, connectorDef);

            BLangSymbol actionSymbol;
            SymbolName name = new SymbolName("NativeAction." + connectorName
                    + ".<init>", connectorDef.getPackagePath());
            actionSymbol = nativeScope.resolve(name);
            if (actionSymbol != null) {
                if (actionSymbol instanceof NativeUnitProxy) {
                    AbstractNativeAction nativeUnit = (AbstractNativeAction) ((NativeUnitProxy) actionSymbol).load();
                    BallerinaAction.BallerinaActionBuilder ballerinaActionBuilder = new BallerinaAction
                            .BallerinaActionBuilder(connectorDef);
                    ballerinaActionBuilder.setIdentifier(nativeUnit.getIdentifier());
                    ballerinaActionBuilder.setPkgPath(nativeUnit.getPackagePath());
                    ballerinaActionBuilder.setNative(nativeUnit.isNative());
                    ballerinaActionBuilder.setSymbolName(nativeUnit.getSymbolName());
                    ParameterDef paramDef = new ParameterDef(connectorDef.getNodeLocation(), null,
                            new Identifier(nativeUnit.getArgumentNames()[0]),
                            nativeUnit.getArgumentTypeNames()[0],
                            new SymbolName(nativeUnit.getArgumentNames()[0], connectorDef.getPackagePath()),
                            ballerinaActionBuilder.getCurrentScope());
                    paramDef.setType(connectorDef);
                    ballerinaActionBuilder.addParameter(paramDef);
                    BallerinaAction ballerinaAction = ballerinaActionBuilder.buildAction();
                    ballerinaAction.setNativeAction((NativeUnitProxy) actionSymbol);
                    ballerinaAction.setConnectorDef(connectorDef);
                    BType bType = BTypes.resolveType(paramDef.getTypeName(), currentScope, paramDef.getNodeLocation());
                    ballerinaAction.setParameterTypes(new BType[]{bType});
                    connectorDef.setInitAction(ballerinaAction);
                }
            }
        }

        for (BallerinaConnectorDef connectorDef : connectorDefArray) {
            // Define actions
            openScope(connectorDef);

            for (BallerinaAction bAction : connectorDef.getActions()) {
                bAction.setConnectorDef(connectorDef);
                defineAction(bAction, connectorDef);
            }

            closeScope();
        }
    }

    private void defineAction(BallerinaAction action, BallerinaConnectorDef connectorDef) {
        //ConnectorDef is a reserved first parameter in any action
        ParameterDef[] updatedParamDefs = new ParameterDef[action.getParameterDefs().length + 1];
        ParameterDef connectorParamDef = new ParameterDef(connectorDef.getNodeLocation(), null,
                new Identifier(TypeConstants.CONNECTOR_TNAME),
                new SimpleTypeName(connectorDef.getName(), null, connectorDef.getPackagePath()),
                new SymbolName(TypeConstants.CONNECTOR_TNAME, connectorDef.getPackagePath()),
                action.getSymbolScope());
        connectorParamDef.setType(connectorDef);
        updatedParamDefs[0] = connectorParamDef;
        for (int i = 0; i < action.getParameterDefs().length; i++) {
            updatedParamDefs[i + 1] = action.getParameterDefs()[i];
        }
        action.setParameterDefs(updatedParamDefs);

        ParameterDef[] paramDefArray = action.getParameterDefs();
        BType[] paramTypes = new BType[paramDefArray.length];
        for (int i = 0; i < paramDefArray.length; i++) {
            ParameterDef paramDef = paramDefArray[i];
            BType bType = BTypes.resolveType(paramDef.getTypeName(), currentScope, paramDef.getNodeLocation());
            paramDef.setType(bType);
            paramTypes[i] = bType;
        }

        action.setParameterTypes(paramTypes);
        ActionSymbolName symbolName = LangModelUtils.getActionSymName(action.getName(), action.getPackagePath(),
                connectorDef.getName(), paramTypes);
        action.setSymbolName(symbolName);

        BLangSymbol actionSymbol = currentScope.resolve(symbolName);
        if (actionSymbol != null) {
            BLangExceptionHelper.throwSemanticError(action, SemanticErrors.REDECLARED_SYMBOL, action.getName());
        }
        currentScope.define(symbolName, action);

        if (action.isNative()) {
            ActionSymbolName nativeActionSymName = LangModelUtils.getNativeActionSymName(action.getName(),
                    connectorDef.getName(), action.getPackagePath(), paramTypes);
            BLangSymbol nativeAction = nativeScope.resolve(nativeActionSymName);

            if (nativeAction == null || !(nativeAction instanceof NativeUnitProxy)) {
                BLangExceptionHelper.throwSemanticError(connectorDef,
                        SemanticErrors.UNDEFINED_NATIVE_ACTION, action.getName(), connectorDef.getName());
                return;
            }

            action.setNativeAction((NativeUnitProxy) nativeAction);
        }

        // Resolve return parameters
        ParameterDef[] returnParameters = action.getReturnParameters();
        BType[] returnTypes = new BType[returnParameters.length];
        for (int i = 0; i < returnParameters.length; i++) {
            ParameterDef paramDef = returnParameters[i];
            BType bType = BTypes.resolveType(paramDef.getTypeName(), currentScope, paramDef.getNodeLocation());
            paramDef.setType(bType);
            returnTypes[i] = bType;
        }
        action.setReturnParamTypes(returnTypes);


        if (action.getWorkers().length > 0) {
            defineWorkers(action.getWorkers(), action);
        }
    }

    private void defineServices(Service[] services) {
        for (Service service : services) {

            // Define Service Symbol in the package scope..
            if (currentScope.resolve(service.getSymbolName()) != null) {
                BLangExceptionHelper.throwSemanticError(service, SemanticErrors.REDECLARED_SYMBOL, service.getName());
            }
            currentScope.define(service.getSymbolName(), service);

            // Define resources
            openScope(service);

            for (Resource resource : service.getResources()) {
                defineResource(resource, service);
            }

            closeScope();
        }
    }

    private void defineResource(Resource resource, Service service) {
        ParameterDef[] paramDefArray = resource.getParameterDefs();
        BType[] paramTypes = new BType[paramDefArray.length];
        for (int i = 0; i < paramDefArray.length; i++) {
            ParameterDef paramDef = paramDefArray[i];
            BType bType = BTypes.resolveType(paramDef.getTypeName(), currentScope, paramDef.getNodeLocation());
            paramDef.setType(bType);
            paramTypes[i] = bType;
        }

        resource.setParameterTypes(paramTypes);
        SymbolName symbolName = LangModelUtils.getResourceSymName(resource.getName(),
                resource.getPackagePath(), service.getName());
        resource.setSymbolName(symbolName);

        if (currentScope.resolve(symbolName) != null) {
            BLangExceptionHelper.throwSemanticError(resource, SemanticErrors.REDECLARED_SYMBOL, resource.getName());
        }
        currentScope.define(symbolName, resource);

        if (resource.getWorkers().length > 0) {
            defineWorkers(resource.getWorkers(), resource);
        }
    }

    private void defineStructs(StructDef[] structDefs) {
        for (StructDef structDef : structDefs) {

            SymbolName symbolName = new SymbolName(structDef.getName(), structDef.getPackagePath());
            // Check whether this constant is already defined.
            if (currentScope.resolve(symbolName) != null) {
                BLangExceptionHelper.throwSemanticError(structDef,
                        SemanticErrors.REDECLARED_SYMBOL, structDef.getName());
            }

            currentScope.define(symbolName, structDef);

            // Create the '<init>' function and inject it to the struct
            BlockStmt.BlockStmtBuilder blockStmtBuilder = new BlockStmt.BlockStmtBuilder(
                    structDef.getNodeLocation(), structDef);
            for (VariableDefStmt variableDefStmt : structDef.getFieldDefStmts()) {
                blockStmtBuilder.addStmt(variableDefStmt);
            }

            BallerinaFunction.BallerinaFunctionBuilder functionBuilder =
                    new BallerinaFunction.BallerinaFunctionBuilder(structDef);
            functionBuilder.setNodeLocation(structDef.getNodeLocation());
            functionBuilder.setIdentifier(new Identifier(structDef + ".<init>"));
            functionBuilder.setPkgPath(structDef.getPackagePath());
            blockStmtBuilder.setBlockKind(StatementKind.CALLABLE_UNIT_BLOCK);
            functionBuilder.setBody(blockStmtBuilder.build());
            structDef.setInitFunction(functionBuilder.buildFunction());
        }

        // Define fields in each struct. This is done after defining all the structs,
        // since a field of a struct can be another struct.
        for (StructDef structDef : structDefs) {
            SymbolScope tmpScope = currentScope;
            currentScope = structDef;
            for (VariableDefStmt fieldDefStmt : structDef.getFieldDefStmts()) {
                fieldDefStmt.getVariableDef().setKind(VariableDef.Kind.STRUCT_FIELD);
                fieldDefStmt.accept(this);
            }
            currentScope = tmpScope;
        }

        // Add type mappers for each struct. This is done after defining all the fields of all the structs,
        // since fields of structs are compared when adding type mappers.
        for (StructDef structDef : structDefs) {
            TypeLattice.addStructEdges(structDef, currentScope);
        }
    }

    /**
     * Add the annotation definitions to the current scope.
     *
     * @param annotationDefs Annotations definitions list
     */
    private void defineAnnotations(AnnotationDef[] annotationDefs) {
        for (AnnotationDef annotationDef : annotationDefs) {
            SymbolName symbolName = new SymbolName(annotationDef.getName(), currentPkg);

            // Check whether this annotation is already defined.
            if (currentScope.resolve(symbolName) != null) {
                BLangExceptionHelper.throwSemanticError(annotationDef,
                        SemanticErrors.REDECLARED_SYMBOL, annotationDef.getSymbolName().getName());
            }

            currentScope.define(symbolName, annotationDef);
        }
    }

    /**
     * Create the '<init>' function and inject it to the connector.
     *
     * @param connectorDef connector model object
     */
    private void createConnectorInitFunction(BallerinaConnectorDef connectorDef) {
        NodeLocation location = connectorDef.getNodeLocation();
        BallerinaFunction.BallerinaFunctionBuilder functionBuilder =
                new BallerinaFunction.BallerinaFunctionBuilder(connectorDef);
        functionBuilder.setNodeLocation(location);
        functionBuilder.setIdentifier(new Identifier(connectorDef.getName() + ".<init>"));
        functionBuilder.setPkgPath(connectorDef.getPackagePath());

        ParameterDef paramDef = new ParameterDef(location, null, new Identifier("connector"),
                null, new SymbolName("connector"), functionBuilder.getCurrentScope());
        paramDef.setType(connectorDef);
        functionBuilder.addParameter(paramDef);

        BlockStmt.BlockStmtBuilder blockStmtBuilder = new BlockStmt.BlockStmtBuilder(location, connectorDef);

        for (VariableDefStmt variableDefStmt : connectorDef.getVariableDefStmts()) {
            AssignStmt assignStmt = new AssignStmt(variableDefStmt.getNodeLocation(),
                    new Expression[]{variableDefStmt.getLExpr()}, variableDefStmt.getRExpr());
            blockStmtBuilder.addStmt(assignStmt);
        }

        // Adding the return statement
        ReturnStmt returnStmt = new ReturnStmt(location, null, new Expression[0]);
        blockStmtBuilder.addStmt(returnStmt);
        blockStmtBuilder.setBlockKind(StatementKind.CALLABLE_UNIT_BLOCK);
        functionBuilder.setBody(blockStmtBuilder.build());
        connectorDef.setInitFunction(functionBuilder.buildFunction());
    }

    /**
     * Create the '<init>' function and inject it to the service.
     *
     * @param service service model object
     */
    private void createServiceInitFunction(Service service) {
        NodeLocation location = service.getNodeLocation();
        BallerinaFunction.BallerinaFunctionBuilder functionBuilder =
                new BallerinaFunction.BallerinaFunctionBuilder(service);
        functionBuilder.setNodeLocation(location);
        functionBuilder.setIdentifier(new Identifier(service.getName() + ".<init>"));
        functionBuilder.setPkgPath(service.getPackagePath());

        BlockStmt.BlockStmtBuilder blockStmtBuilder = new BlockStmt.BlockStmtBuilder(location, service);
        for (VariableDefStmt variableDefStmt : service.getVariableDefStmts()) {
            AssignStmt assignStmt = new AssignStmt(variableDefStmt.getNodeLocation(),
                    new Expression[]{variableDefStmt.getLExpr()}, variableDefStmt.getRExpr());
            blockStmtBuilder.addStmt(assignStmt);
        }

        // Adding the return statement
        ReturnStmt returnStmt = new ReturnStmt(location, null, new Expression[0]);
        blockStmtBuilder.addStmt(returnStmt);
        blockStmtBuilder.setBlockKind(StatementKind.CALLABLE_UNIT_BLOCK);
        functionBuilder.setBody(blockStmtBuilder.build());
        service.setInitFunction(functionBuilder.buildFunction());
    }

    private void resolveStructFieldTypes(StructDef[] structDefs) {
        for (StructDef structDef : structDefs) {
            for (VariableDefStmt fieldDefStmt : structDef.getFieldDefStmts()) {
                VariableDef fieldDef = fieldDefStmt.getVariableDef();
                BType fieldType = BTypes.resolveType(fieldDef.getTypeName(), currentScope,
                        fieldDef.getNodeLocation());
                fieldDef.setType(fieldType);
            }
        }
    }

    private void checkUnreachableStmt(Statement[] stmts, int stmtIndex) {
        if (stmts.length > stmtIndex) {
            //skip comment statement.
            if (stmts[stmtIndex] instanceof CommentStmt) {
                checkUnreachableStmt(stmts, ++stmtIndex);
            } else {
                BLangExceptionHelper.throwSemanticError(stmts[stmtIndex], SemanticErrors.UNREACHABLE_STATEMENT);
            }
        }
    }

    /**
     * Recursively visits a nested init expression. Reconstruct the init expression with the
     * specific init expression type, and replaces the generic {@link RefTypeInitExpr}.
     *
     * @param fieldType Type of the current field
     * @return reconstructed nested init expression
     */
    private RefTypeInitExpr getNestedInitExpr(Expression expr, BType fieldType) {
        RefTypeInitExpr refTypeInitExpr = (RefTypeInitExpr) expr;
        if (refTypeInitExpr instanceof ArrayInitExpr) {
            if (fieldType == BTypes.typeAny || fieldType == BTypes.typeMap) {
                fieldType = BTypes.resolveType(new SimpleTypeName(BTypes.typeAny.getName(),
                        true, 1), currentScope, expr.getNodeLocation());
            } else if (getElementType(fieldType) == BTypes.typeJSON) {
                refTypeInitExpr = new JSONArrayInitExpr(refTypeInitExpr.getNodeLocation(),
                        refTypeInitExpr.getWhiteSpaceDescriptor(), refTypeInitExpr.getArgExprs());
            }
        } else {
            // if the inherited type is any, then default this initializer to a map init expression
            if (fieldType == BTypes.typeAny) {
                fieldType = BTypes.typeMap;
            }
            if (fieldType == BTypes.typeMap) {
                refTypeInitExpr = new MapInitExpr(refTypeInitExpr.getNodeLocation(),
                        refTypeInitExpr.getWhiteSpaceDescriptor(), refTypeInitExpr.getArgExprs());
            } else if (fieldType == BTypes.typeJSON || fieldType instanceof BJSONConstraintType) {
                refTypeInitExpr = new JSONInitExpr(refTypeInitExpr.getNodeLocation(),
                        refTypeInitExpr.getWhiteSpaceDescriptor(), refTypeInitExpr.getArgExprs());
            } else if (fieldType instanceof StructDef) {
                refTypeInitExpr = new StructInitExpr(refTypeInitExpr.getNodeLocation(),
                        refTypeInitExpr.getWhiteSpaceDescriptor(), refTypeInitExpr.getArgExprs());
            }

            if (refTypeInitExpr instanceof ConnectorInitExpr) {
                ConnectorInitExpr filterConnectorInitExpr = ((ConnectorInitExpr) refTypeInitExpr).
                        getParentConnectorInitExpr();
                BType type = null;
                while (filterConnectorInitExpr != null) {
                    BLangSymbol symbol = currentPackageScope.resolve(new SymbolName(filterConnectorInitExpr.
                            getTypeName().getName(), currentPkg));
                    if (symbol instanceof BallerinaConnectorDef) {
                        type = (BType) symbol;
                        filterConnectorInitExpr.setInheritedType(type);

                        type = BTypes.resolveType(((BallerinaConnectorDef) symbol).
                                getFilterSupportedType(), currentScope, refTypeInitExpr.getNodeLocation());
                        if (type != null) {
                            filterConnectorInitExpr.setFilterSupportedType(type);
                        }
                    }
                    filterConnectorInitExpr = (filterConnectorInitExpr).
                            getParentConnectorInitExpr();
                }
            }
        }

        refTypeInitExpr.setInheritedType(fieldType);
        return refTypeInitExpr;
    }

    private BType getElementType(BType type) {
        if (type.getTag() != TypeTags.ARRAY_TAG) {
            return type;
        }

        return getElementType(((BArrayType) type).getElementType());
    }

    /**
     * Visit and validate map/json initialize expression.
     *
     * @param initExpr Expression to visit.
     */
    private void visitMapJsonInitExpr(RefTypeInitExpr initExpr) {
        BType inheritedType = initExpr.getInheritedType();
        initExpr.setType(inheritedType);
        Expression[] argExprs = initExpr.getArgExprs();

        for (int i = 0; i < argExprs.length; i++) {
            Expression argExpr = argExprs[i];
            KeyValueExpr keyValueExpr = (KeyValueExpr) argExpr;
            Expression keyExpr = keyValueExpr.getKeyExpr();

            // In maps and json, key is always a string literal.
            if (keyExpr instanceof SimpleVarRefExpr) {
                BString key = new BString(((SimpleVarRefExpr) keyExpr).getVarName());
                keyExpr = new BasicLiteral(keyExpr.getNodeLocation(), keyExpr.getWhiteSpaceDescriptor(),
                        new SimpleTypeName(TypeConstants.STRING_TNAME),
                        key);
                keyValueExpr.setKeyExpr(keyExpr);
            }
            visitSingleValueExpr(keyExpr);

            Expression valueExpr = keyValueExpr.getValueExpr();
            if (inheritedType instanceof BJSONConstraintType) {
                String key = ((BasicLiteral) keyExpr).getBValue().stringValue();
                StructDef constraintStructDef = (StructDef) ((BJSONConstraintType) inheritedType).getConstraint();
                if (constraintStructDef != null) {
                    BLangSymbol varDefSymbol = constraintStructDef.resolveMembers(
                            new SymbolName(key, constraintStructDef.getPackagePath()));
                    if (varDefSymbol == null) {
                        throw BLangExceptionHelper.getSemanticError(keyExpr.getNodeLocation(),
                                SemanticErrors.UNKNOWN_FIELD_IN_JSON_STRUCT, key, constraintStructDef.getName());
                    }
                    VariableDef varDef = (VariableDef) varDefSymbol;
                    BType cJSONFieldType = new BJSONConstraintType(varDef.getType());
                    if (valueExpr instanceof RefTypeInitExpr) {
                        valueExpr = getNestedInitExpr(valueExpr, cJSONFieldType);
                        keyValueExpr.setValueExpr(valueExpr);
                    }
                }
            } else {
                if (valueExpr instanceof RefTypeInitExpr) {
                    valueExpr = getNestedInitExpr(valueExpr, inheritedType);
                    keyValueExpr.setValueExpr(valueExpr);
                }
            }

            valueExpr.accept(this);
            BType valueExprType = valueExpr.getType();

            // Generate type cast expression if the rhs type is a value type
            if (inheritedType == BTypes.typeMap) {
                if (BTypes.isValueType(valueExprType)) {
                    TypeCastExpression newExpr = checkWideningPossible(BTypes.typeAny, valueExpr);
                    if (newExpr != null) {
                        keyValueExpr.setValueExpr(newExpr);
                    } else {
                        BLangExceptionHelper.throwSemanticError(keyValueExpr,
                                SemanticErrors.INCOMPATIBLE_TYPES_CANNOT_CONVERT, valueExprType.getSymbolName(),
                                inheritedType);
                    }
                }
                continue;
            }

            // for JSON init expr, check the type compatibility of the value.
            if (BTypes.isValueType(valueExprType)) {
                TypeCastExpression typeCastExpr = checkWideningPossible(BTypes.typeJSON, valueExpr);
                if (typeCastExpr != null) {
                    keyValueExpr.setValueExpr(typeCastExpr);
                } else {
                    BLangExceptionHelper.throwSemanticError(keyValueExpr,
                            SemanticErrors.INCOMPATIBLE_TYPES_CANNOT_CONVERT, valueExprType.getSymbolName(),
                            inheritedType.getSymbolName());
                }
                continue;
            }

            if (valueExprType != BTypes.typeNull && isAssignableTo(BTypes.typeJSON, valueExprType)) {
                continue;
            }

            TypeCastExpression typeCastExpr = checkWideningPossible(BTypes.typeJSON, valueExpr);
            if (typeCastExpr == null) {
                BLangExceptionHelper.throwSemanticError(initExpr, SemanticErrors.INCOMPATIBLE_TYPES_CANNOT_CONVERT,
                        valueExpr.getType(), BTypes.typeJSON);
            }
            keyValueExpr.setValueExpr(typeCastExpr);
        }

    }

    private void addDependentPkgInitCalls(List<BallerinaFunction> initFunctionList,
                                          BlockStmt.BlockStmtBuilder blockStmtBuilder, NodeLocation initFuncLocation) {
        for (BallerinaFunction initFunc : initFunctionList) {
            FunctionInvocationExpr funcIExpr = new FunctionInvocationExpr(initFuncLocation, null,
                    initFunc.getName(), null, initFunc.getPackagePath(), new Expression[]{});
            funcIExpr.setCallableUnit(initFunc);
            FunctionInvocationStmt funcIStmt = new FunctionInvocationStmt(initFuncLocation, funcIExpr);
            blockStmtBuilder.addStmt(funcIStmt);
        }
    }

    private boolean isAssignableTo(BType lhsType, BType rhsType) {
        if (lhsType == BTypes.typeAny) {
            return true;
        }

        if (rhsType == BTypes.typeNull && !BTypes.isValueType(lhsType)) {
            return true;
        }

        if (lhsType == BTypes.typeJSON && rhsType.getTag() == TypeTags.C_JSON_TAG) {
            return true;
        }

        return lhsType == rhsType || lhsType.equals(rhsType);
    }

    private boolean checkUnsafeCastPossible(BType sourceType, BType targetType) {

        // 1) If either source type or target type is of type 'any', then an unsafe cast possible;
        if (sourceType == BTypes.typeAny || targetType == BTypes.typeAny) {
            return true;
        }

        // 2) If both types are struct types, unsafe cast is possible
        if (sourceType instanceof StructDef && targetType instanceof StructDef) {
            return true;
        }

        // 3) If both types are not array types, unsafe cast is not possible now.
        if (targetType.getTag() == TypeTags.ARRAY_TAG || sourceType.getTag() == TypeTags.ARRAY_TAG) {
            return isUnsafeArrayCastPossible(sourceType, targetType);
        }

        if (sourceType.getTag() == TypeTags.JSON_TAG && targetType.getTag() == TypeTags.C_JSON_TAG) {
            return true;
        }

        return false;
    }

    private boolean isUnsafeArrayCastPossible(BType sourceType, BType targetType) {
        if (targetType.getTag() == TypeTags.ARRAY_TAG && sourceType.getTag() == TypeTags.ARRAY_TAG) {
            BArrayType sourceArrayType = (BArrayType) sourceType;
            BArrayType targetArrayType = (BArrayType) targetType;
            return isUnsafeArrayCastPossible(sourceArrayType.getElementType(), targetArrayType.getElementType());

        } else if (targetType.getTag() == TypeTags.ARRAY_TAG) {

            if (sourceType == BTypes.typeJSON) {
                return isUnsafeArrayCastPossible(BTypes.typeJSON, ((BArrayType) targetType).getElementType());
            }

            // If only the target type is an array type, then the source type must be of type 'any'
            return sourceType == BTypes.typeAny;

        } else if (sourceType.getTag() == TypeTags.ARRAY_TAG) {

            if (targetType == BTypes.typeJSON) {
                return isUnsafeArrayCastPossible(((BArrayType) sourceType).getElementType(), BTypes.typeJSON);
            }

            // If only the source type is an array type, then the target type must be of type 'any'
            return targetType == BTypes.typeAny;
        }

        // Now both types are not array types
        if (sourceType == targetType) {
            return true;
        }

        // In this case, target type should be of type 'any' and the source type cannot be a value type
        if (targetType == BTypes.typeAny && !BTypes.isValueType(sourceType)) {
            return true;
        }

        return !BTypes.isValueType(targetType) && sourceType == BTypes.typeAny;
    }

    private AssignabilityResult performAssignabilityCheck(BType lhsType, Expression rhsExpr) {
        AssignabilityResult assignabilityResult = new AssignabilityResult();
        BType rhsType = rhsExpr.getType();
        if (lhsType == rhsType) {
            assignabilityResult.assignable = true;
            return assignabilityResult;
        }

        if (rhsType == BTypes.typeNull && !BTypes.isValueType(lhsType)) {
            assignabilityResult.assignable = true;
            return assignabilityResult;
        }

        if ((rhsType instanceof BJSONConstraintType) && (lhsType == BTypes.typeJSON)) {
            assignabilityResult.assignable = true;
            return assignabilityResult;
        }

        if ((rhsType instanceof BJSONConstraintType) && (lhsType instanceof BJSONConstraintType)) {
            if (((BJSONConstraintType) lhsType).getConstraint() == ((BJSONConstraintType) rhsType).getConstraint()) {
                assignabilityResult.assignable = true;
                return assignabilityResult;
            }
        }

        // Now check whether an implicit cast is available;
        TypeCastExpression implicitCastExpr = checkWideningPossible(lhsType, rhsExpr);
        if (implicitCastExpr != null) {
            assignabilityResult.assignable = true;
            assignabilityResult.expression = implicitCastExpr;
            return assignabilityResult;
        }

        // Now check whether left-hand side type is 'any', then an implicit cast is possible;
        if (isImplicitiCastPossible(lhsType, rhsType)) {
            implicitCastExpr = new TypeCastExpression(rhsExpr.getNodeLocation(),
                    null, rhsExpr, lhsType);
            implicitCastExpr.setOpcode(InstructionCodes.NOP);

            assignabilityResult.assignable = true;
            assignabilityResult.expression = implicitCastExpr;
            return assignabilityResult;
        }

        if (lhsType == BTypes.typeFloat && rhsType == BTypes.typeInt && rhsExpr instanceof BasicLiteral) {
            BasicLiteral newExpr = new BasicLiteral(rhsExpr.getNodeLocation(), rhsExpr.getWhiteSpaceDescriptor(),
                    new SimpleTypeName(TypeConstants.FLOAT_TNAME), new BFloat(((BasicLiteral) rhsExpr)
                    .getBValue().intValue()));
            visitSingleValueExpr(newExpr);
            assignabilityResult.assignable = true;
            assignabilityResult.expression = newExpr;
            return assignabilityResult;
        }

        // Further check whether types are assignable recursively, specially array types
        if (rhsType instanceof BFunctionType && lhsType instanceof BFunctionType) {
            BFunctionType rhs = (BFunctionType) rhsType;
            BFunctionType lhs = (BFunctionType) lhsType;
            if (rhs.getParameterType().length == lhs.getParameterType().length &&
                    rhs.getReturnParameterType().length == lhs.getReturnParameterType().length) {
                for (int i = 0; i < rhs.getParameterType().length; i++) {
                    if (!isAssignableTo(rhs.getParameterType()[i], lhs.getParameterType()[i])) {
                        return assignabilityResult;
                    }
                }
                for (int i = 0; i < rhs.getReturnParameterType().length; i++) {
                    if (!isAssignableTo(rhs.getReturnParameterType()[i], lhs.getReturnParameterType()[i])) {
                        return assignabilityResult;
                    }
                }
                assignabilityResult.assignable = true;
                return assignabilityResult;
            }
        }
        return assignabilityResult;
    }

    private boolean isImplicitiCastPossible(BType lhsType, BType rhsType) {
        if (lhsType == BTypes.typeAny) {
            return true;
        }

        // 2) Check whether both types are array types
        if (lhsType.getTag() == TypeTags.ARRAY_TAG || rhsType.getTag() == TypeTags.ARRAY_TAG) {
            return isImplicitArrayCastPossible(lhsType, rhsType);
        }

        return false;
    }

    private boolean isImplicitArrayCastPossible(BType lhsType, BType rhsType) {
        if (lhsType.getTag() == TypeTags.ARRAY_TAG && rhsType.getTag() == TypeTags.ARRAY_TAG) {
            // Both types are array types
            BArrayType lhrArrayType = (BArrayType) lhsType;
            BArrayType rhsArrayType = (BArrayType) rhsType;
            return isImplicitArrayCastPossible(lhrArrayType.getElementType(), rhsArrayType.getElementType());

        } else if (rhsType.getTag() == TypeTags.ARRAY_TAG) {
            // Only the right-hand side is an array type
            // Then lhs type should 'any' type
            return lhsType == BTypes.typeAny;

        } else if (lhsType.getTag() == TypeTags.ARRAY_TAG) {
            // Only the left-hand side is an array type
            return false;
        }

        // Now both types are not array types
        if (lhsType == rhsType) {
            return true;
        }

        // In this case, lhs type should be of type 'any' and the rhs type cannot be a value type
        return lhsType.getTag() == BTypes.typeAny.getTag() && !BTypes.isValueType(rhsType);
    }

    /**
     * Helper method to add return statement if required.
     *
     * @param callableUnit action/function.
     */
    private void checkAndAddReturnStmt(CallableUnit callableUnit) {
        BlockStmt blockStmt = callableUnit.getCallableUnitBody();
        ParameterDef[] retParams = callableUnit.getReturnParameters();
        if (retParams.length > 0 && !blockStmt.isAlwaysReturns()) {
            throw BLangExceptionHelper.getSemanticError(callableUnit.getNodeLocation(),
                    SemanticErrors.MISSING_RETURN_STATEMENT);
        } else if (blockStmt.isAlwaysReturns()) {
            return;
        }

        Statement[] statements = blockStmt.getStatements();
        int length = statements.length;
        NodeLocation blockLocation = blockStmt.getNodeLocation();
        NodeLocation endOfBlock = new NodeLocation(blockLocation.getPackageDirPath(),
                blockLocation.getFileName(), blockLocation.stopLineNumber);
        ReturnStmt returnStmt = new ReturnStmt(endOfBlock, null, new Expression[0]);

        int lengthWithReturn = length + 1;
        statements = Arrays.copyOf(statements, lengthWithReturn);
        statements[lengthWithReturn - 1] = returnStmt;
        blockStmt.setStatements(statements);
    }

    private void checkAndAddReplyStmt(BlockStmt blockStmt) {
        Statement[] statements = blockStmt.getStatements();
        int length = statements.length;
        if ((statements.length > 0 && !(statements[length - 1] instanceof ReplyStmt)) || statements.length == 0) {
            NodeLocation blockLocation = blockStmt.getNodeLocation();
            NodeLocation endOfBlock = new NodeLocation(blockLocation.getPackageDirPath(),
                    blockLocation.getFileName(), blockLocation.stopLineNumber);
            ReplyStmt replyStmt = new ReplyStmt(endOfBlock, null, null);
            statements = Arrays.copyOf(statements, length + 1);
            statements[length] = replyStmt;
            blockStmt.setStatements(statements);
        }
    }

    private void assignVariableRefTypes(Expression[] expr, BType[] returnTypes) {
        for (int i = 0; i < expr.length; i++) {
            if (expr[i] instanceof SimpleVarRefExpr && ((SimpleVarRefExpr) expr[i]).getVarName().equals("_")) {
                continue;
            }
            ((SimpleVarRefExpr) expr[i]).getVariableDef().setType(returnTypes[i]);
        }
    }

    private static void checkParent(Statement stmt) {
        Statement parent = stmt;
        StatementKind childStmtType = stmt.getKind();
        while (StatementKind.CALLABLE_UNIT_BLOCK != parent.getKind()) {
            if (StatementKind.WHILE_BLOCK == parent.getKind() &&
                    (StatementKind.BREAK == childStmtType || StatementKind.CONTINUE == childStmtType)) {
                return;
            } else if (StatementKind.TRANSACTION_BLOCK == parent.getKind()) {
                if (StatementKind.BREAK == childStmtType) {
                    BLangExceptionHelper.throwSemanticError(stmt, SemanticErrors.BREAK_USED_IN_TRANSACTION);
                } else if (StatementKind.CONTINUE == childStmtType) {
                    BLangExceptionHelper.throwSemanticError(stmt, SemanticErrors.CONTINUE_USED_IN_TRANSACTION);
                }
            }
            parent = parent.getParent();
        }
    }

    private static void checkRetryStmtValidity(RetryStmt stmt) {
        //Check whether the retry statement is root level statement in the failed block
        StatementKind parentStmtType = stmt.getParent().getKind();
        if (StatementKind.FAILED_BLOCK != parentStmtType) {
            BLangExceptionHelper.throwSemanticError(stmt, SemanticErrors.INVALID_RETRY_STMT_LOCATION);
        }
        //Only non negative integer constants and integer literals are allowed as retry count;
        Expression retryCountExpr = stmt.getRetryCountExpression();
        boolean error = true;
        if (retryCountExpr instanceof BasicLiteral) {
            if (retryCountExpr.getType().getTag() == TypeTags.INT_TAG) {
                if (((BasicLiteral) retryCountExpr).getBValue().intValue() >= 0) {
                    error = false;
                }
            }
        } else if (retryCountExpr instanceof VariableReferenceExpr) {
            VariableDef variableDef = ((SimpleVarRefExpr) retryCountExpr).getVariableDef();
            if (variableDef.getKind() == VariableDef.Kind.CONSTANT) {
                if (variableDef.getType().getTag() == TypeTags.INT_TAG) {
                    error = false;
                }
            }
        }
        if (error) {
            BLangExceptionHelper.throwSemanticError(stmt, SemanticErrors.INVALID_RETRY_COUNT);
        }
    }

    /**
     * Get the XML namespaces that are visible to to the current scope.
     *
     * @param location Source location of the ballerina file
     * @return XML namespaces that are visible to the current scope, as a map
     */
    private Map<String, Expression> getNamespaceInScope(NodeLocation location) {
        Map<String, Expression> namespaces = new HashMap<String, Expression>();
        SymbolScope scope = currentScope;
        while (true) {
            for (Entry<SymbolName, BLangSymbol> symbols : scope.getSymbolMap().entrySet()) {
                SymbolName symbolName = symbols.getKey();
                if (!(symbolName instanceof NamespaceSymbolName)) {
                    continue;
                }

                NamespaceDeclaration namespaceDecl = (NamespaceDeclaration) symbols.getValue();
                if (!namespaces.containsKey(namespaceDecl.getPrefix())
                        && !namespaces.containsValue(namespaceDecl.getNamespaceUri())) {

                    BasicLiteral namespaceUriLiteral =
                            new BasicLiteral(location, null, new SimpleTypeName(TypeConstants.STRING_TNAME),
                                    new BString(namespaceDecl.getNamespaceUri()));
                    namespaceUriLiteral.accept(this);
                    namespaces.put(namespaceDecl.getPrefix(), namespaceUriLiteral);
                }
            }

            if (scope instanceof BLangPackage) {
                break;
            }
            scope = scope.getEnclosingScope();
        }

        return namespaces;
    }

    /**
     * Create and return an XML concatenation expression using using the provided expressions.
     * Expressions can only be either XML type or string type. All the string type expressions
     * will be converted to XML text literals ({@link XMLTextLiteral}).
     *
     * @param items Expressions to create concatenating expression.
     * @return XML concatenating expression
     */
    private Expression getXMLConcatExpression(Expression[] items) {
        if (items.length == 0) {
            return null;
        }

        Expression concatExpr = null;
        for (int i = 0; i < items.length; i++) {
            Expression currentItem = items[i];
            if (currentItem.getType() == BTypes.typeString) {
                currentItem = new XMLTextLiteral(currentItem.getNodeLocation(), currentItem.getWhiteSpaceDescriptor(),
                        currentItem);
                items[0] = currentItem;
            }

            if (concatExpr == null) {
                concatExpr = currentItem;
                continue;
            }

            concatExpr = new AddExpression(currentItem.getNodeLocation(), currentItem.getWhiteSpaceDescriptor(),
                    concatExpr, currentItem);
            concatExpr.setType(BTypes.typeXML);
        }

        return concatExpr;
    }

    private void validateXMLQname(XMLQNameExpr qname, Map<String, Expression> namespaces, Expression defaultNsUri) {
        qname.setType(BTypes.typeString);
        String prefix = qname.getPrefix();

        if (prefix.isEmpty()) {
            qname.setNamepsaceUri(defaultNsUri);
            return;
        }

        if (namespaces.containsKey(qname.getPrefix())) {
            Expression namespaceUri = namespaces.get(qname.getPrefix());
            qname.setNamepsaceUri(namespaceUri);
        } else if (prefix.equals(XMLConstants.XMLNS_ATTRIBUTE)) {
            BLangExceptionHelper.throwSemanticError(qname, SemanticErrors.INVALID_NAMESPACE_PREFIX, prefix);
        } else {
            BLangExceptionHelper.throwSemanticError(qname, SemanticErrors.UNDEFINED_NAMESPACE, qname.getPrefix());
        }
    }

    private void validateXMLLiteralAttributes(List<KeyValueExpr> attributes, Map<String, Expression> namespaces) {
        // Validate attributes
        for (KeyValueExpr attribute : attributes) {
            Expression attrNameExpr = attribute.getKeyExpr();

            if (attrNameExpr instanceof XMLQNameExpr) {
                XMLQNameExpr attrQNameRefExpr = (XMLQNameExpr) attrNameExpr;
                attrQNameRefExpr.isUsedInXML();

                BasicLiteral emptyNsUriLiteral = new BasicLiteral(attrNameExpr.getNodeLocation(), null,
                        new SimpleTypeName(TypeConstants.STRING_TNAME), new BString(XMLConstants.NULL_NS_URI));
                emptyNsUriLiteral.accept(this);
                validateXMLQname(attrQNameRefExpr, namespaces, emptyNsUriLiteral);
            } else {
                attrNameExpr.accept(this);
                if (attrNameExpr.getType() != BTypes.typeString) {
                    attrNameExpr =
                            createImplicitStringConversionExpr(attrNameExpr, attrNameExpr.getType());
                    attribute.setKeyExpr(attrNameExpr);
                }
            }

            Expression attrValueExpr = attribute.getValueExpr();
            attrValueExpr.accept(this);
            if (attrValueExpr.getType() != BTypes.typeString) {
                attrValueExpr = createImplicitStringConversionExpr(attrValueExpr, attrValueExpr.getType());
                attribute.setValueExpr(attrValueExpr);
            }
        }
    }

    private void validateXMLLiteralEndTag(XMLElementLiteral xmlElementLiteral, Expression defaultNsUri) {
        Expression startTagName = xmlElementLiteral.getStartTagName();
        Expression endTagName = xmlElementLiteral.getEndTagName();

        // Compare start and end tags
        if (endTagName != null) {
            if (startTagName instanceof XMLQNameExpr && endTagName instanceof XMLQNameExpr) {
                XMLQNameExpr startName = (XMLQNameExpr) startTagName;
                XMLQNameExpr endName = (XMLQNameExpr) endTagName;
                if (!startName.getPrefix().equals(endName.getPrefix())
                        || !startName.getLocalname().equals(endName.getLocalname())) {
                    BLangExceptionHelper.throwSemanticError(endTagName, SemanticErrors.XML_TAGS_MISMATCH);
                }
            }

            if (((startTagName instanceof XMLQNameExpr) && !(endTagName instanceof XMLQNameExpr))
                    || (!(startTagName instanceof XMLQNameExpr) && (endTagName instanceof XMLQNameExpr))) {
                BLangExceptionHelper.throwSemanticError(endTagName, SemanticErrors.XML_TAGS_MISMATCH);
            }

            if (endTagName instanceof XMLQNameExpr) {
                validateXMLQname((XMLQNameExpr) endTagName, xmlElementLiteral.getNamespaces(), defaultNsUri);
            } else {
                endTagName.accept(this);
            }

            if (endTagName.getType() != BTypes.typeString) {
                endTagName = createImplicitStringConversionExpr(endTagName, endTagName.getType());
                xmlElementLiteral.setEndTagName(endTagName);
            }
        }
    }

    private Expression createImplicitStringConversionExpr(Expression sExpr, BType sType) {
        Expression conversionExpr = getImplicitConversionExpr(sExpr, sType, BTypes.typeString);
        if (conversionExpr == null) {
            BLangExceptionHelper.throwSemanticError(sExpr, SemanticErrors.INCOMPATIBLE_TYPES, BTypes.typeString, sType);
        }
        return conversionExpr;
    }

    /**
     * This class holds the results of the type assignability check.
     *
     * @since 0.88
     */
    static class AssignabilityResult {
        boolean assignable;
        Expression expression;
    }
}
