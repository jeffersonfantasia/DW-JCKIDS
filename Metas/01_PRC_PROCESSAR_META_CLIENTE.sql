CREATE OR REPLACE PROCEDURE PROCESSAR_META_CLIENTES IS
    v_file UTL_FILE.FILE_TYPE;
    v_buffer VARCHAR2(4000);
    v_cod_rede_cliente VARCHAR2(10);
    v_data_inicial DATE;
    v_data_final DATE;
    v_valor_meta NUMBER(18, 6);
    v_valor_meta_diaria NUMBER(18, 6);
    v_line VARCHAR2(4000);
    v_data DATE;
    v_num_dias NUMBER;

    PROCEDURE inserir_ou_atualizar_meta(p_data DATE, p_cod_rede_cliente VARCHAR2, p_valor_meta NUMBER) IS
    BEGIN
        BEGIN
            UPDATE BI_SINC_META_CLIENTE
            SET VLMETA = p_valor_meta,
                DT_UPDATE = SYSDATE
            WHERE DATA = p_data
            AND CODCLIREDE = p_cod_rede_cliente;

            IF SQL%ROWCOUNT = 0 THEN
                INSERT INTO BI_SINC_META_CLIENTE(DATA, CODCLIREDE, VLMETA, DT_UPDATE)
                VALUES (p_data, p_cod_rede_cliente, p_valor_meta, SYSDATE);
            END IF;
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('Erro ao processar o registro: ' || SQLERRM);
        END;
    END inserir_ou_atualizar_meta;

BEGIN
    -- Abrir o arquivo CSV
    v_file := UTL_FILE.FOPEN('PLANILHAS_DIR', 'MetaClientes.csv', 'R');

    -- Ler o cabeçalho
    UTL_FILE.GET_LINE(v_file, v_buffer);

    -- Loop para ler cada linha do arquivo
    LOOP
        BEGIN
            UTL_FILE.GET_LINE(v_file, v_line);
            -- Extrair os valores das colunas
            v_cod_rede_cliente := SUBSTR(v_line, 1, INSTR(v_line, ',', 1, 1) - 1);
            v_line := SUBSTR(v_line, INSTR(v_line, ',', 1, 1) + 1);
            v_data_inicial := TO_DATE(SUBSTR(v_line, 1, INSTR(v_line, ',', 1, 1) - 1), 'DD/MM/YYYY');
            v_line := SUBSTR(v_line, INSTR(v_line, ',', 1, 1) + 1);
            v_data_final := TO_DATE(SUBSTR(v_line, 1, INSTR(v_line, ',', 1, 1) - 1), 'DD/MM/YYYY');
            v_line := SUBSTR(v_line, INSTR(v_line, ',', 1, 1) + 1);
            v_valor_meta := TO_NUMBER(v_line);

            -- Calcular o número de dias no período (inclusivo)
            v_num_dias := v_data_final - v_data_inicial + 1;

            -- Calcular a meta diária
            v_valor_meta_diaria := v_valor_meta / v_num_dias;

            -- Processamento para dividir a meta por dia
            v_data := v_data_inicial;
            WHILE v_data <= v_data_final LOOP
                inserir_ou_atualizar_meta(v_data, v_cod_rede_cliente, v_valor_meta_diaria);
                v_data := v_data + 1;
            END LOOP;

        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                EXIT;
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('Erro ao ler o arquivo: ' || SQLERRM);
                EXIT;
        END;
    END LOOP;

    -- Fechar o arquivo
    UTL_FILE.FCLOSE(v_file);

    COMMIT;
END;
