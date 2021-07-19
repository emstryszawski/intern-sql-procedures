use CDN_Firma_Demo
go
-- Procedura zlicza sta¿ w czasie nieobecnoœci
alter procedure CDN.NS_ZliczNieob 
	@PraId int, @DataOd datetime, @DataDo datetime,
	@Lata int output, @Mies int output, @Dni int output
as
	declare @NieobOd datetime
	declare @NieobDo datetime

	set @Dni = 0
	set @Mies = 0
	set @Lata = 0

	declare KursorNieob cursor for
		select PNB_OkresOd, PNB_OkresDo from CDN.PracNieobec
		join CDN.TypNieobec on PNB_TnbId = TNB_TnbId
		where 
			PNB_PraId = @PraId and
			PNB_TnbId NOT IN (17, 61, 62, 48, 49) and
			PNB_OkresOd between @DataOd and @DataDo or
			@DataOd between PNB_OkresOd and PNB_OkresDo
			-- jesli okres Do przekracza zatrudnienie to przyciac nieobecnosc

begin
	open KursorNieob
	fetch next from KursorNieob into @NieobOd, @NieobDo

	while @@FETCH_STATUS = 0 begin
		
		if @NieobDo > @DataDo and @NieobOd between @DataOd and @DataDo
			set @NieobDo = @DataDo

		else if @NieobOd < @DataOd and @NieobDo between @DataOd and @DataDo
			set @NieobOd = @DataOd

		exec CDN.StazCalc @NieobOd, @NieobDo, @Lata output, @Mies output, @Dni output

		fetch next from KursorNieob into @NieobOd, @NieobDo
	end

	close KursorNieob
	deallocate KursorNieob
end
go

-- Procedura licz¹ca sta¿ z E i U
alter procedure CDN.NS_staz2 @PraId int, @Data datetime, 
@Lata int output, @Mies int output, @Dni int output,
@Lata2 int output, @Mies2 int output, @Dni2 int output,
@Lata3 int output, @Mies3 int output, @Dni3 int output
as

--DROP TABLE IF EXISTS #Daty
CREATE TABLE #Daty (id int PRIMARY KEY identity (1,1),
	PraId int,
	TypUmowy int,
	ZatrudnionyOd datetime, ZatrudnionyDo datetime)

declare @Typ int
declare @ZatrOd datetime
declare @ZatrDo datetime

declare kursorEtaty cursor for
	select PRI_PraId, PRI_Typ, PRE_ZatrudnionyOd, PRE_ZatrudnionyDo
	from CDN.Pracidx
	join CDN.PracEtaty on PRI_PraId = PRE_PraId
	where PRI_Typ = 10 and PRI_PraId = @PraId and PRE_DataOd <= @Data
	union all
	select PRI_PraId, PRI_Typ, UMW_DataOd, UMW_DataDo
	from CDN.Pracidx
	join CDN.Umowy on PRI_PraId = UMW_PraId
	where PRI_Typ = 20 and PRI_PraId = @PraId and UMW_DataOd <= @Data
	order by PRE_ZatrudnionyOd

begin
	open kursorEtaty
	fetch next from kursorEtaty into @PraId, @Typ, @ZatrOd, @ZatrDo
	while @@FETCH_STATUS = 0  begin
			
		if @ZatrOd = convert(Datetime,'1900-01-01 00:00:00.000',120)
			continue
		if @ZatrDo > @Data
			set @ZatrDo = @Data

		if not exists (select * from #Daty)
			insert into #Daty values (@PraId, @Typ, @ZatrOd, @ZatrDo)
		else begin

			declare @BeforeRekord int = (select max(id) from #Daty)
			declare @PoprzedniTyp int  = (select TypUmowy from #Daty where id = @BeforeRekord)
			declare @PoprzednieZatrOd datetime = (select ZatrudnionyOd from #Daty where id = @BeforeRekord)
			declare @PoprzednieZatrDo datetime = (select ZatrudnionyDo from #Daty where id = @BeforeRekord)

			if (@PoprzedniTyp = 10 and @Typ = 10) or (@PoprzedniTyp = 20 and @Typ = 20) begin

				if @ZatrOd between @PoprzednieZatrOd and @PoprzednieZatrDo begin

					if @ZatrDo <> @PoprzednieZatrDo begin
						update #Daty set ZatrudnionyDo = @ZatrDo where id = @BeforeRekord
					end
				end
				else if @ZatrOd > @PoprzednieZatrDo begin
					insert into #Daty values (@PraId, @Typ, @ZatrOd, @ZatrDo) 
				end
			end
			else if @PoprzedniTyp = 10 and @Typ = 20 begin

				if @ZatrOd between @PoprzednieZatrOd and @PoprzednieZatrDo begin

					if @ZatrDo > @PoprzednieZatrOd and @ZatrDo < @PoprzednieZatrDo begin
						fetch next from kursorEtaty into @PraId, @Typ, @ZatrOd, @ZatrDo
						set @BeforeRekord = (select max(id) from #Daty)
						set @PoprzedniTyp = (select TypUmowy from #Daty where id = @BeforeRekord)
						set @PoprzednieZatrOd = (select ZatrudnionyOd from #Daty where id = @BeforeRekord)
						set @PoprzednieZatrDo = (select ZatrudnionyDo from #Daty where id = @BeforeRekord)
					end
					else if @ZatrDo > @PoprzednieZatrDo
						insert into #Daty values (@PraId, @Typ, @PoprzednieZatrDo, @ZatrDo)
				end
				else if @ZatrOd > @PoprzednieZatrDo
					insert into #Daty values (@PraId, @Typ, @ZatrOd, @ZatrDo)
			end
			else if @PoprzedniTyp = 20 and @Typ = 10 begin

				if @ZatrOd between @PoprzednieZatrOd and @PoprzednieZatrDo begin

					if @ZatrDo < @PoprzednieZatrDo begin

 						-- trzeba rozdzielic na dwie umowy
						insert into #Daty values 
							(@PraId, @Typ, @ZatrOd, @ZatrDo),
							(@PraId, @PoprzedniTyp, @ZatrDo, @PoprzednieZatrDo)
						update #Daty set ZatrudnionyDo = @ZatrOd where id = @BeforeRekord
							
					end
					else if @ZatrDo >= @PoprzednieZatrDo begin
						
						insert into #Daty values (@PraId, @Typ, @ZatrOd, @ZatrDo)
						update #Daty set ZatrudnionyDo = @ZatrOd where id = @BeforeRekord
					end
				end
				else if @ZatrOd > @PoprzednieZatrDo
					insert into #Daty values (@PraId, @Typ, @ZatrOd, @ZatrDo)
			end
		end

		fetch next from kursorEtaty into @PraId, @Typ, @ZatrOd, @ZatrDo
	end
	close kursorEtaty
	deallocate kursorEtaty

	--DROP TABLE IF EXISTS #Staz
	CREATE TABLE #Staz (id int PRIMARY KEY identity (1,1),
	PraId int,
	TypUmowy int,
	Lata int, Mies int, Dni int)

	declare @NLata int
	declare @NMies int
	declare @NDni int

	set @NLata = 0
	set @NMies = 0
	set @NDni = 0

	--declare @Lata int
	--declare @Mies int
	--declare @Dni int

	set @Lata = 0
	set @Mies = 0
	set @Dni = 0

	declare NieobKursor cursor for 
	select TypUmowy, ZatrudnionyOd, ZatrudnionyDo from #Daty
	where PraId = @PraId

	open NieobKursor

	fetch next from NieobKursor into @Typ, @ZatrOd, @ZatrDo
	while @@FETCH_STATUS = 0 begin
		exec CDN.NS_ZliczNieob @PraId, @ZatrOd, @ZatrDo, @NLata output, @NMies output, @NDni output
		exec CDN.StazCalc @ZatrOd, @ZatrDo, @Lata output, @Mies output, @Dni output

		IF  @NLata<>0 or @NMies<>0 or @NDni<>0 BEGIN
		SET @Lata = @Lata - @NLata

		IF @Mies>=@NMies
			SET @Mies = @Mies - @NMies

		ELSE IF @Mies<@NMies BEGIN
			SET @Lata = @Lata-1
			SET @Mies = (@Mies+12)- @NMies
		END
		IF @Dni>=@NDni
			SET @Dni = @Dni - @NDni

		ELSE IF @Dni<@NDni and @Mies>0 BEGIN
			SET @Mies = @Mies - 1
			SET @Dni = (@Dni+30) - @NDni
		END
		ELSE IF @Dni<@NDni and @Mies=0 BEGIN
			SET @Lata = @Lata-1
			SET @Mies = 11
			SET @Dni = (@Dni+30) - @NDni
		END
	END

		insert into #Staz values (@PraId, @Typ, @Lata, @Mies, @Dni)
		set @NLata = 0
		set @NMies = 0
		set @NDni = 0
		set @Lata = 0
		set @Mies = 0
		set @Dni = 0
		fetch next from NieobKursor into @Typ, @ZatrOd, @ZatrDo
	end

	close NieobKursor
	deallocate NieobKursor

	set @Lata = (select ISNULL(SUM(Lata), 0) from #Staz where TypUmowy = 10)
	set @Mies = (select ISNULL(SUM(Mies), 0) from #Staz where TypUmowy = 10) 
	set @Dni = (select ISNULL(SUM(Dni), 0) from #Staz where TypUmowy = 10)
	set @Lata2 = (select ISNULL(SUM(Lata), 0) from #Staz where TypUmowy = 20)
	set @Mies2 = (select ISNULL(SUM(Mies), 0) from #Staz where TypUmowy = 20)
	set @Dni2 = (select ISNULL(SUM(Dni), 0) from #Staz where TypUmowy = 20)

	SET @Mies3 = @Mies +@Mies2
	SET @Dni3 = @Dni + @Dni2
	SET @Lata3 = @Lata +@Lata2

	SET @Mies = @Mies + @Dni/30
	SET @Dni = @Dni%30
	SET @Lata = @Lata + @Mies/12
	SET @Mies = @Mies%12

	SET @Mies2 = @Mies2 + @Dni2/30
	SET @Dni2 = @Dni2%30
	SET @Lata2 = @Lata2 + @Mies2/12
	SET @Mies2 = @Mies2%12

	SET @Mies3 = @Mies3 + @Dni3/30
	SET @Dni3 = @Dni3%30
	SET @Lata3 = @Lata3 + @Mies3/12
	SET @Mies3 = @Mies3%12

	
	--select * from #Daty
drop table #Daty
drop table #Staz	
end
go

--Wylicza sta¿ dla wszystkich pracowników
alter procedure CDN.NS_StazEtatUmowa @Data datetime
as
declare @Lata int
declare @Mies int
declare @Dni int
declare @Lata2 int
declare @Mies2 int
declare @Dni2 int
declare @Lata3 int
declare @Mies3 int
declare @Dni3 int
declare @PraId int
declare @Kod varchar(20)
declare @Nazwisko nvarchar(40)
declare @Imie nvarchar(30)

--DROP TABLE IF EXISTS #PracownicyStaze
CREATE TABLE #PracownicyStaze (id int PRIMARY KEY identity (1,1),
	PraId int,
	Kod varchar(20),
	Nazwisko nvarchar(40),
	Imie nvarchar(30),
	EtatLata int,
	EtatMies int,
	EtatDni int,
	UmowaLata int,
	UmowaMies int,
	UmowaDni int,
	OgolemLata int,
	OgolemMies int,
	OgolemDni int)

declare PracownicyKursor cursor for
select distinct PRI_PraId, PRI_Kod, PRI_Nazwisko, PRI_Imie1 from CDN.Pracidx
where PRI_Typ IN (10, 20)

begin

	open PracownicyKursor
	fetch next from PracownicyKursor into @PraId, @Kod, @Nazwisko, @Imie

	WHILE @@FETCH_STATUS = 0	
	BEGIN  

	  exec CDN.NS_staz2 @PraId, @Data, @Lata output, @Mies output, @Dni output, @Lata2 output, @Mies2 output, @Dni2 output, @Lata3 output, @Mies3 output, @Dni3 output

	  insert into #PracownicyStaze values (@PraId, @Kod, @Nazwisko, @Imie, @Lata, @Mies, @Dni, @Lata2, @Mies2, @Dni2, @Lata3, @Mies3, @Dni3)

	  fetch next from PracownicyKursor into @PraId, @Kod, @Nazwisko, @Imie
	END
	close PracownicyKursor
	deallocate PracownicyKursor


	select * from #PracownicyStaze where Kod is Not Null order by Nazwisko,Imie,Kod
	drop table #PracownicyStaze
end
go